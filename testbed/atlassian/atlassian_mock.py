"""📚 Эмулятор Confluence, Jira и Bitbucket Server/DC для тестового стенда.
*2026-10-03 14:46 MSK · v1.3 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Зачем. GigaCode ходит в корпоративный Confluence (их несколько: confluence.delta.sbrf.ru,
confluence.sberbank.ru) и в Jira через MCP `mcp-atlassian`, по экземпляру на сервер. Чтобы
навыки, которые ссылаются на Confluence, проверялись на стенде, нужен сервер с тем же REST:
этот эмулятор отвечает на то, что спрашивает `mcp-atlassian` 0.23 (набор снят прогоном его
инструментов чтения; на неизвестный путь — 404 и строка в журнале).

Bitbucket (stash.delta.sbrf.ru, stash.sigma.sbrf.ru) — то же для MCP
`@atlassian-dc-mcp/bitbucket` и для форка `mcp-atlassian-with-bitbucket` (REST под `/rest/api/1.0`,
файл и каталог — JSON `/browse`): агент читает DDL и код по ссылке на репозиторий.

Один код — несколько экземпляров (`atlassian-mock@.service`): `ATLASSIAN_MOCK_SERVER` =
`delta` / `sber` (Confluence), `jira` или `stash-delta` / `stash-sigma` (Bitbucket). Только
чтение, вход по Bearer PAT, как на Server/DC.

Наполнение — каталог `ATLASSIAN_MOCK_FIXTURES`:

    confluence/<server>/<SPACE>/<id>.md   страница: шапка YAML (title, parent, labels) + Markdown
    confluence/<server>/<SPACE>/<id>.html та же шапка + готовый XHTML (импорт HTML-экспорта
                                          Confluence — `import_html_export.py`)
    confluence/<server>/<SPACE>/_space.yaml  имя и описание пространства (необязательно)
    jira/issues.json                      задачи: key, summary, description, status, labels, …
    bitbucket/<server>/<PROJECT>/<slug>.git  bare-клон репозитория (git clone --bare)

Id страниц, ключи пространств и названия, ключи проектов и slug — как в корпоративных серверах: тогда ссылка
вида `/pages/viewpage.action?pageId=…` или `/display/<SPACE>/<Title>` из навыка находит
страницу и здесь; так же `/projects/<P>/repos/<r>/browse/<path>` находит файл. Корпоративное содержимое живёт только на стенде, в git его нет.

Запуск (юнит делает то же самое):

    ATLASSIAN_MOCK_SERVER=delta ATLASSIAN_MOCK_FIXTURES=/opt/aftest/atlassian-mock/fixtures \\
    ATLASSIAN_MOCK_TOKENS=<pat> /opt/aftest/venv/bin/python -m uvicorn atlassian_mock:app \\
        --host 127.0.0.1 --port 8090

⚠️ Это эмулятор: CQL и JQL — подмножество (условия через AND/OR без скобок, ORDER BY по
дате и ключу), поиск по тексту — все слова запроса в названии или теле.
"""
from __future__ import annotations

import html
import json
import logging
import os
import re
import shlex
import subprocess
import zlib
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote_plus

import yaml
from markdown_it import MarkdownIt
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

logger = logging.getLogger('atlassian_mock')

SERVER = os.getenv('ATLASSIAN_MOCK_SERVER', 'delta')
FIXTURES = Path(os.getenv('ATLASSIAN_MOCK_FIXTURES', Path(__file__).with_name('fixtures')))
TOKENS = {t.strip() for t in os.getenv('ATLASSIAN_MOCK_TOKENS', '').split(',') if t.strip()}
#: От имени кого «написаны» страницы и задачи: поле автора у Server/DC обязательно.
USER = {'type': 'known', 'username': 'stand', 'userKey': 'stand', 'displayName': 'Стенд HR-платформы',
        'name': 'stand', 'key': 'stand', 'emailAddress': 'stand@stand.test', 'active': True}
PAGE_SIZE = 25


# ── Наполнение ───────────────────────────────────────────────────────────────

def _mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec='milliseconds')


def _split_front_matter(text: str) -> tuple[dict, str]:
    if text.startswith('---\n'):
        head, _, body = text[4:].partition('\n---\n')
        return yaml.safe_load(head) or {}, body
    return {}, text


@lru_cache(maxsize=1)
def confluence() -> tuple[dict, dict]:
    """(пространства, страницы) сервера. Кэш на процесс: поменял фикстуры — перезапусти юнит."""
    md = MarkdownIt('commonmark').enable('table')
    spaces, pages = {}, {}
    root = FIXTURES / 'confluence' / SERVER
    for space_dir in sorted(p for p in root.glob('*') if p.is_dir()):
        key = space_dir.name
        meta_file = space_dir / '_space.yaml'
        meta = yaml.safe_load(meta_file.read_text(encoding='utf-8')) if meta_file.exists() else {}
        spaces[key] = {'id': zlib.crc32(key.encode()) % 10**8, 'key': key, 'name': meta.get('name', key),
                       'description': meta.get('description', ''), 'type': 'global'}
        for f in sorted([*space_dir.glob('*.md'), *space_dir.glob('*.html')]):
            head, body = _split_front_matter(f.read_text(encoding='utf-8'))
            is_html = f.suffix == '.html'
            pid = str(head.get('id', f.stem))
            pages[pid] = {
                'id': pid, 'space': key, 'title': str(head.get('title', f.stem)),
                'parent': str(head['parent']) if head.get('parent') else None,
                'labels': [str(x) for x in head.get('labels', [])],
                'storage': body if is_html else md.render(body),
                'text': html.unescape(re.sub(r'<[^>]+>', ' ', body)) if is_html else body,
                'updated': _mtime(f),
            }
    return spaces, pages


@lru_cache(maxsize=1)
def jira_issues() -> list[dict]:
    path = FIXTURES / 'jira' / 'issues.json'
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else []


# ── Разбор CQL и JQL ─────────────────────────────────────────────────────────

_CLAUSE = re.compile(r'^\s*([\w.]+)\s*(!=|!~|~|=|\bnot in\b|\bin\b)\s*(.+?)\s*$', re.I)


def parse_query(query: str) -> tuple[list[list[tuple[str, str, list[str]]]], str | None]:
    """Запрос → (ИЛИ из И условий, ORDER BY). Условие — (поле, оператор, значения)."""
    query = query or ''
    order = None
    if m := re.search(r'\border\s+by\s+(.+)$', query, re.I):
        order, query = m.group(1).strip(), query[:m.start()]
    query = query.replace('(', ' ( ').replace(')', ' ) ') if ' in ' in query.lower() else query
    groups = []
    for part in re.split(r'\s+or\s+', query.strip(), flags=re.I):
        clauses = []
        for raw in re.split(r'\s+and\s+', part, flags=re.I):
            raw = raw.strip().strip('()').strip()
            if not raw:
                continue
            m = _CLAUSE.match(raw)
            if not m:
                logger.warning('условие не разобрано, пропускаю: %r', raw)
                continue
            field, op, value = m.group(1).lower(), m.group(2).lower(), m.group(3).strip()
            values = [v for v in shlex.split(value.strip('()').replace(',', ' '))] if 'in' in op else [
                shlex.split(value)[0] if value[:1] in '"\'' else value]
            clauses.append((field, op, values))
        if clauses:
            groups.append(clauses)
    return groups, order


def _words_in(needle: str, haystack: str) -> bool:
    words = re.findall(r'\w+', needle.lower())
    hay = haystack.lower()
    return all(w in hay for w in words)


def _match(groups, check) -> bool:
    return not groups or any(all(check(*c) for c in clauses) for clauses in groups)


def _cmp(op: str, actual, values: list[str]) -> bool:
    actual = actual if isinstance(actual, list) else [actual]
    actual = [str(a).lower() for a in actual if a is not None]
    values = [v.lower() for v in values]
    hit = any(v in actual for v in values)
    return not hit if op in ('!=', 'not in') else hit


# ── Confluence: представление страницы ───────────────────────────────────────

def _space_json(key: str) -> dict:
    s = confluence()[0][key]
    return {**{k: s[k] for k in ('id', 'key', 'name', 'type')},
            'description': {'plain': {'value': s['description'], 'representation': 'plain'}},
            '_links': {'webui': f'/display/{key}', 'self': f'/rest/api/space/{key}'}}


def _ancestors(page: dict) -> list[dict]:
    pages = confluence()[1]
    out, pid = [], page['parent']
    while pid and pid in pages and len(out) < 20:
        out.insert(0, {'id': pid, 'type': 'page', 'status': 'current', 'title': pages[pid]['title']})
        pid = pages[pid]['parent']
    return out


def _webui(page: dict) -> str:
    return f'/pages/viewpage.action?pageId={page["id"]}'


def page_json(page: dict, expand: str = '') -> dict:
    version = {'number': 1, 'when': page['updated'], 'by': USER, 'minorEdit': False}
    out = {
        'id': page['id'], 'type': 'page', 'status': 'current', 'title': page['title'],
        'space': _space_json(page['space']),
        'version': version,
        'history': {'latest': True, 'createdBy': USER, 'createdDate': page['updated']},
        'ancestors': _ancestors(page),
        'metadata': {'labels': {'results': [{'prefix': 'global', 'name': lb, 'id': lb} for lb in page['labels']],
                                'size': len(page['labels'])}},
        'extensions': {'position': 'none'},
        '_links': {'webui': _webui(page), 'tinyui': _webui(page), 'self': f'/rest/api/content/{page["id"]}'},
        '_expandable': {},
    }
    if 'body' in expand or not expand:
        out['body'] = {'storage': {'value': page['storage'], 'representation': 'storage'},
                       'view': {'value': page['storage'], 'representation': 'view'}}
    return out


def _page_matches(field: str, op: str, values: list[str], page: dict) -> bool:
    if field in ('text', 'siteSearch'.lower(), 'content'):
        hit = any(_words_in(v, page['title'] + '\n' + page['text']) for v in values)
        return not hit if op.startswith('!') else hit
    if field == 'title':
        if op in ('~', '!~'):
            hit = any(_words_in(v, page['title']) for v in values)
            return not hit if op == '!~' else hit
        return _cmp(op, page['title'], values)
    if field in ('space', 'space.key'):
        return _cmp(op, page['space'], values)
    if field == 'label':
        return _cmp(op, page['labels'], values)
    if field in ('ancestor', 'parent'):
        chain = [a['id'] for a in _ancestors(page)] if field == 'ancestor' else [page['parent']]
        return _cmp(op, chain, values)
    if field in ('id', 'content.id'):
        return _cmp(op, page['id'], values)
    if field == 'type':
        return _cmp(op, 'page', values)
    logger.warning('поле CQL %r не поддержано — условие считаю выполненным', field)
    return True


def cql_pages(cql: str) -> list[dict]:
    groups, order = parse_query(cql)
    found = [p for p in confluence()[1].values()
             if _match(groups, lambda f, o, v, p=p: _page_matches(f, o, v, p))]
    reverse = bool(order and 'desc' in order.lower())
    key = (lambda p: p['updated']) if order and 'modified' in order.lower() else (lambda p: p['title'])
    return sorted(found, key=key, reverse=reverse)


def _paged(request: Request, items: list) -> tuple[list, int, int]:
    start = int(request.query_params.get('start', 0) or 0)
    limit = int(request.query_params.get('limit', PAGE_SIZE) or PAGE_SIZE)
    return items[start:start + limit], start, limit


# ── Confluence: маршруты ─────────────────────────────────────────────────────

def _not_found(what: str) -> JSONResponse:
    return JSONResponse({'statusCode': 404, 'message': f'не найдено: {what}'}, status_code=404)


async def content_get(request: Request):
    page = confluence()[1].get(request.path_params['id'])
    if not page:
        return _not_found(f'страница {request.path_params["id"]}')
    return JSONResponse(page_json(page, request.query_params.get('expand', '')))


async def content_list(request: Request):
    qp = request.query_params
    pages = [p for p in confluence()[1].values()
             if (not qp.get('spaceKey') or p['space'] == qp['spaceKey'])
             and (not qp.get('title') or p['title'] == qp['title'])]
    chunk, start, limit = _paged(request, sorted(pages, key=lambda p: p['title']))
    return JSONResponse({'results': [page_json(p, qp.get('expand', '')) for p in chunk],
                         'start': start, 'limit': limit, 'size': len(chunk)})


async def content_search(request: Request):
    pages = cql_pages(request.query_params.get('cql', ''))
    chunk, start, limit = _paged(request, pages)
    return JSONResponse({'results': [page_json(p, request.query_params.get('expand', '')) for p in chunk],
                         'start': start, 'limit': limit, 'size': len(chunk), 'totalSize': len(pages)})


def _excerpt(page: dict, cql: str) -> str:
    words = re.findall(r'"([^"]+)"', cql) or ['']
    text = re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', ' ', page['storage'])))
    pos = max(text.lower().find(words[0].lower().split()[0]) if words[0] else 0, 0)
    return text[max(pos - 80, 0):pos + 220]


async def search(request: Request):
    cql = request.query_params.get('cql', '')
    pages = cql_pages(cql)
    chunk, start, limit = _paged(request, pages)
    results = [{
        'content': page_json(p, request.query_params.get('expand', '')),
        'title': p['title'], 'excerpt': _excerpt(p, cql), 'url': _webui(p),
        'resultGlobalContainer': {'title': confluence()[0][p['space']]['name'], 'displayUrl': f'/display/{p["space"]}'},
        'entityType': 'content', 'lastModified': p['updated'], 'friendlyLastModified': p['updated'][:10],
    } for p in chunk]
    return JSONResponse({'results': results, 'start': start, 'limit': limit, 'size': len(results),
                         'totalSize': len(pages), 'cqlQuery': cql})


async def children(request: Request):
    kind = request.path_params.get('kind', 'page')
    if kind != 'page':
        return JSONResponse({'results': [], 'start': 0, 'limit': PAGE_SIZE, 'size': 0})
    pid = request.path_params['id']
    kids = sorted((p for p in confluence()[1].values() if p['parent'] == pid), key=lambda p: p['title'])
    chunk, start, limit = _paged(request, kids)
    return JSONResponse({'results': [page_json(p, request.query_params.get('expand', '')) for p in chunk],
                         'start': start, 'limit': limit, 'size': len(chunk)})


async def child_any(request: Request):
    """child?expand=page — то, чем atlassian-python-api спрашивает потомков всех типов."""
    pid = request.path_params['id']
    kids = [page_json(p, '') for p in confluence()[1].values() if p['parent'] == pid]
    empty = {'results': [], 'start': 0, 'limit': PAGE_SIZE, 'size': 0}
    return JSONResponse({'page': {'results': kids, 'start': 0, 'limit': PAGE_SIZE, 'size': len(kids)},
                         'comment': empty, 'attachment': empty})


async def labels(request: Request):
    page = confluence()[1].get(request.path_params['id'])
    if not page:
        return _not_found(f'страница {request.path_params["id"]}')
    res = [{'prefix': 'global', 'name': lb, 'id': lb, 'label': lb} for lb in page['labels']]
    return JSONResponse({'results': res, 'start': 0, 'limit': PAGE_SIZE, 'size': len(res)})


async def empty_list(request: Request):
    return JSONResponse({'results': [], 'start': 0, 'limit': PAGE_SIZE, 'size': 0})


async def spaces(request: Request):
    items = [_space_json(k) for k in confluence()[0]]
    chunk, start, limit = _paged(request, items)
    return JSONResponse({'results': chunk, 'start': start, 'limit': limit, 'size': len(chunk)})


async def space_get(request: Request):
    key = request.path_params['key']
    return JSONResponse(_space_json(key)) if key in confluence()[0] else _not_found(f'пространство {key}')


async def current_user(request: Request):
    return JSONResponse(USER)


async def group_members(request: Request):
    return JSONResponse({'results': [USER], 'start': 0, 'limit': PAGE_SIZE, 'size': 1})


async def user_search(request: Request):
    return JSONResponse({'results': [{'user': USER, 'title': USER['displayName'], 'entityType': 'user'}],
                         'start': 0, 'limit': PAGE_SIZE, 'size': 1, 'totalSize': 1})


# ── Веб-ссылки: то, что открывает человек по ссылке из навыка ────────────────

def _page_html(page: dict) -> HTMLResponse:
    crumbs = ' / '.join(html.escape(a['title']) for a in _ancestors(page))
    body = (f'<!doctype html><meta charset="utf-8"><title>{html.escape(page["title"])}</title>'
            f'<p><small>{html.escape(page["space"])} {("/ " + crumbs) if crumbs else ""}</small></p>'
            f'<h1>{html.escape(page["title"])}</h1>{page["storage"]}')
    return HTMLResponse(body)


async def viewpage(request: Request):
    page = confluence()[1].get(request.query_params.get('pageId', ''))
    return _page_html(page) if page else HTMLResponse('страница не найдена', status_code=404)


async def display(request: Request):
    key, title = request.path_params['key'], unquote_plus(request.path_params['title'])
    page = next((p for p in confluence()[1].values() if p['space'] == key and p['title'] == title), None)
    return _page_html(page) if page else HTMLResponse('страница не найдена', status_code=404)


# ── Jira ─────────────────────────────────────────────────────────────────────

def issue_json(issue: dict) -> dict:
    project = issue['key'].rsplit('-', 1)[0]
    status = issue.get('status', 'Open')
    comments = [{'id': str(i), 'author': USER, 'body': c, 'created': issue.get('updated', issue.get('created')),
                 'updated': issue.get('updated', issue.get('created'))} for i, c in enumerate(issue.get('comments', []), 1)]
    return {
        'id': str(zlib.crc32(issue['key'].encode()) % 10**7), 'key': issue['key'], 'self': f'/rest/api/2/issue/{issue["key"]}',
        'fields': {
            'summary': issue.get('summary', ''), 'description': issue.get('description', ''),
            'status': {'name': status, 'statusCategory': {'key': 'done' if status in ('Done', 'Closed') else 'new',
                                                          'name': status}},
            'issuetype': {'name': issue.get('type', 'Task'), 'subtask': False},
            'priority': {'name': issue.get('priority', 'Medium')},
            'project': {'key': project, 'name': project},
            'labels': issue.get('labels', []), 'components': [], 'fixVersions': [],
            'created': issue.get('created'), 'updated': issue.get('updated', issue.get('created')),
            'reporter': USER, 'assignee': USER, 'creator': USER,
            'comment': {'comments': comments, 'total': len(comments), 'maxResults': len(comments), 'startAt': 0},
            'issuelinks': [], 'subtasks': [], 'attachment': [], 'parent': None,
        },
    }


def _issue_matches(field: str, op: str, values: list[str], issue: dict) -> bool:
    if field == 'project':
        return _cmp(op, issue['key'].rsplit('-', 1)[0], values)
    if field in ('key', 'issuekey', 'id'):
        return _cmp(op, issue['key'], values)
    if field in ('text', 'summary', 'description'):
        hay = issue.get('summary', '') if field == 'summary' else (
            issue.get('description', '') if field == 'description'
            else ' '.join([issue.get('summary', ''), issue.get('description', ''), *issue.get('comments', [])]))
        hit = any(_words_in(v, hay) for v in values)
        return not hit if op.startswith('!') else hit
    if field == 'status':
        return _cmp(op, issue.get('status', 'Open'), values)
    if field in ('issuetype', 'type'):
        return _cmp(op, issue.get('type', 'Task'), values)
    if field in ('labels', 'label'):
        return _cmp(op, issue.get('labels', []), values)
    logger.warning('поле JQL %r не поддержано — условие считаю выполненным', field)
    return True


def jql_issues(jql: str) -> list[dict]:
    groups, order = parse_query(jql)
    found = [i for i in jira_issues() if _match(groups, lambda f, o, v, i=i: _issue_matches(f, o, v, i))]
    reverse = not order or 'desc' in order.lower()
    field = 'created' if order and 'created' in order.lower() else 'updated'
    return sorted(found, key=lambda i: i.get(field) or i.get('created') or '', reverse=reverse)


async def jira_search(request: Request):
    params = dict(request.query_params)
    if request.method == 'POST':
        params.update(await request.json())
    found = jql_issues(params.get('jql', ''))
    start = int(params.get('startAt', 0) or 0)
    limit = int(params.get('maxResults', 50) or 50)
    chunk = found[start:start + limit]
    return JSONResponse({'startAt': start, 'maxResults': limit, 'total': len(found),
                         'issues': [issue_json(i) for i in chunk]})


def _issue(key: str) -> dict | None:
    return next((i for i in jira_issues() if i['key'] == key), None)


async def jira_issue(request: Request):
    issue = _issue(request.path_params['key'])
    return JSONResponse(issue_json(issue)) if issue else JSONResponse(
        {'errorMessages': ['Issue Does Not Exist'], 'errors': {}}, status_code=404)


async def jira_comments(request: Request):
    issue = _issue(request.path_params['key'])
    if not issue:
        return JSONResponse({'errorMessages': ['Issue Does Not Exist'], 'errors': {}}, status_code=404)
    return JSONResponse(issue_json(issue)['fields']['comment'])


def _projects() -> list[dict]:
    keys = sorted({i['key'].rsplit('-', 1)[0] for i in jira_issues()})
    return [{'id': str(n), 'key': k, 'name': k, 'projectTypeKey': 'software',
             'self': f'/rest/api/2/project/{k}'} for n, k in enumerate(keys, 10000)]


async def jira_projects(request: Request):
    return JSONResponse(_projects())


async def jira_project(request: Request):
    p = next((p for p in _projects() if p['key'] == request.path_params['key']), None)
    return JSONResponse(p) if p else JSONResponse({'errorMessages': ['No project'], 'errors': {}}, status_code=404)


FIELDS = [{'id': f, 'key': f, 'name': n, 'custom': False, 'navigable': True, 'searchable': True,
           'schema': {'type': t, 'system': f}}
          for f, n, t in (('summary', 'Summary', 'string'), ('description', 'Description', 'string'),
                          ('status', 'Status', 'status'), ('labels', 'Labels', 'array'),
                          ('created', 'Created', 'datetime'), ('updated', 'Updated', 'datetime'),
                          ('issuetype', 'Issue Type', 'issuetype'), ('project', 'Project', 'project'),
                          ('assignee', 'Assignee', 'user'), ('reporter', 'Reporter', 'user'),
                          ('priority', 'Priority', 'priority'), ('comment', 'Comment', 'comments-page'))]


async def jira_fields(request: Request):
    return JSONResponse(FIELDS)


async def jira_server_info(request: Request):
    return JSONResponse({'baseUrl': str(request.base_url).rstrip('/'), 'version': '9.12.0',
                         'versionNumbers': [9, 12, 0], 'deploymentType': 'Server', 'serverTitle': 'Jira стенда'})


async def jira_myself(request: Request):
    return JSONResponse(USER)


async def jira_transitions(request: Request):
    return JSONResponse({'transitions': []})


async def jira_link_types(request: Request):
    return JSONResponse({'issueLinkTypes': []})


# ── Bitbucket: репозитории — bare-клоны, читаются git'ом ─────────────────────

def _repos() -> dict[tuple[str, str], Path]:
    """(PROJECT, slug) → bare-клон. Без кэша: положил клон — виден сразу."""
    root = FIXTURES / 'bitbucket' / SERVER
    return {(d.parent.name.upper(), d.name[:-4].lower()): d for d in sorted(root.glob('*/*.git'))}


def git(repo: Path, *args: str) -> str:
    # safe.directory: клоны кладутся не тем пользователем, под которым идёт юнит
    out = subprocess.run(['git', '-c', 'safe.directory=*', '-C', str(repo), *args],
                         capture_output=True, text=True, timeout=30)
    if out.returncode:
        raise FileNotFoundError(out.stderr.strip())
    return out.stdout


def _bb_page(request: Request, items: list) -> dict:
    start = int(request.query_params.get('start', 0) or 0)
    limit = int(request.query_params.get('limit', PAGE_SIZE) or PAGE_SIZE)
    chunk = items[start:start + limit]
    last = start + limit >= len(items)
    return {'size': len(chunk), 'limit': limit, 'start': start, 'isLastPage': last, 'values': chunk,
            **({} if last else {'nextPageStart': start + limit})}


def _bb_404(what: str) -> JSONResponse:
    return JSONResponse({'errors': [{'context': None, 'message': f'{what} does not exist.',
                                     'exceptionName': 'com.atlassian.bitbucket.NoSuchEntityException'}]},
                        status_code=404)


def project_json(key: str) -> dict:
    return {'key': key, 'id': zlib.crc32(key.encode()) % 10**5, 'name': key, 'public': False, 'type': 'NORMAL',
            'links': {'self': [{'href': f'/projects/{key}'}]}}


def repo_json(key: str, slug: str) -> dict:
    return {'slug': slug, 'id': zlib.crc32(f'{key}/{slug}'.encode()) % 10**5, 'name': slug, 'scmId': 'git',
            'state': 'AVAILABLE', 'statusMessage': 'Available', 'forkable': True, 'public': False,
            'project': project_json(key),
            'links': {'clone': [{'href': f'/scm/{key.lower()}/{slug}.git', 'name': 'http'}],
                      'self': [{'href': f'/projects/{key}/repos/{slug}/browse'}]}}


def _repo(request: Request) -> tuple[str, str, Path | None]:
    key, slug = request.path_params['key'].upper(), request.path_params['slug'].lower()
    return key, slug, _repos().get((key, slug))


async def bb_projects(request: Request):
    keys = sorted({k for k, _ in _repos()})
    return JSONResponse(_bb_page(request, [project_json(k) for k in keys]))


async def bb_project(request: Request):
    key = request.path_params['key'].upper()
    return JSONResponse(project_json(key)) if any(k == key for k, _ in _repos()) else _bb_404(f'Project {key}')


async def bb_repos(request: Request):
    key = request.path_params.get('key', request.query_params.get('projectkey', '')).upper()
    items = [repo_json(k, s) for k, s in _repos() if not key or k == key]
    return JSONResponse(_bb_page(request, items))


async def bb_repo(request: Request):
    key, slug, repo = _repo(request)
    return JSONResponse(repo_json(key, slug)) if repo else _bb_404(f'Repository {key}/{slug}')


def read_path(repo: Path, path: str, at: str | None) -> str:
    """Файл — его текст; каталог — листинг дерева, как отдаёт /raw Bitbucket на каталог."""
    ref = at or 'HEAD'
    path = path.strip('/')
    if not path or git(repo, 'cat-file', '-t', f'{ref}:{path}').strip() == 'tree':
        return git(repo, 'ls-tree', ref, *([f'{path}/'] if path else []))
    return git(repo, 'show', f'{ref}:{path}')


async def bb_raw(request: Request):
    key, slug, repo = _repo(request)
    if not repo:
        return _bb_404(f'Repository {key}/{slug}')
    path = request.path_params.get('path', '')
    try:
        return PlainTextResponse(read_path(repo, path, request.query_params.get('at')))
    except FileNotFoundError:
        return _bb_404(f'The path "{path}"')


def browse_json(repo: Path, path: str, at: str | None) -> dict:
    """REST /browse, как у Bitbucket DC: файл — строки `lines`, каталог — `children.values`."""
    ref = at or 'HEAD'
    path = path.strip('/')
    if path and git(repo, 'cat-file', '-t', f'{ref}:{path}').strip() != 'tree':
        lines = git(repo, 'show', f'{ref}:{path}').splitlines()
        return {'lines': [{'text': t} for t in lines], 'start': 0, 'size': len(lines), 'isLastPage': True}
    values = []
    for row in git(repo, 'ls-tree', '-l', ref, *([f'{path}/'] if path else [])).splitlines():
        meta, full = row.split('\t', 1)
        _, kind, sha, size = meta.split()
        name = full.rsplit('/', 1)[-1]
        values.append({'path': {'components': [name], 'name': name, 'toString': name}, 'contentId': sha,
                       'type': 'DIRECTORY' if kind == 'tree' else 'FILE',
                       **({} if size == '-' else {'size': int(size)})})
    return {'path': {'components': path.split('/') if path else [], 'toString': path}, 'revision': ref,
            'children': {'size': len(values), 'limit': len(values), 'start': 0, 'isLastPage': True,
                         'values': values}}


async def bb_browse_json(request: Request):
    key, slug, repo = _repo(request)
    if not repo:
        return _bb_404(f'Repository {key}/{slug}')
    path = request.path_params.get('path', '')
    try:
        return JSONResponse(browse_json(repo, path, request.query_params.get('at')))
    except FileNotFoundError:
        return _bb_404(f'The path "{path}"')


async def bb_app_props(request: Request):
    return JSONResponse({'version': '8.19.0', 'buildNumber': '8019000', 'buildDate': '1700000000000',
                         'displayName': 'Bitbucket'})


async def bb_branches(request: Request):
    key, slug, repo = _repo(request)
    if not repo:
        return _bb_404(f'Repository {key}/{slug}')
    head = git(repo, 'symbolic-ref', 'HEAD').strip()
    rows = git(repo, 'for-each-ref', '--format=%(refname)\t%(objectname)', 'refs/heads').splitlines()
    items = [{'id': ref, 'displayId': ref.removeprefix('refs/heads/'), 'type': 'BRANCH', 'latestCommit': sha,
              'latestChangeset': sha, 'isDefault': ref == head}
             for ref, sha in (r.split('\t') for r in rows)]
    return JSONResponse(_bb_page(request, items))


async def bb_commits(request: Request):
    key, slug, repo = _repo(request)
    if not repo:
        return _bb_404(f'Repository {key}/{slug}')
    qp = request.query_params
    rng = qp.get('until') or 'HEAD'
    if qp.get('since'):
        rng = f'{qp["since"]}..{rng}'
    args = ['log', '--format=%H%x1f%h%x1f%an%x1f%ae%x1f%at%x1f%cn%x1f%ce%x1f%ct%x1f%P%x1f%B%x1e', rng]
    if qp.get('path'):
        args += ['--', qp['path']]
    try:
        out = git(repo, *args)
    except FileNotFoundError as e:
        return JSONResponse({'errors': [{'message': str(e)}]}, status_code=404)
    items = []
    for rec in out.split('\x1e'):
        if not rec.strip():
            continue
        sha, short, an, ae, at, cn, ce, ct, parents, msg = rec.strip('\n').split('\x1f')
        items.append({'id': sha, 'displayId': short, 'message': msg.strip(),
                      'author': {'name': an, 'emailAddress': ae}, 'authorTimestamp': int(at) * 1000,
                      'committer': {'name': cn, 'emailAddress': ce}, 'committerTimestamp': int(ct) * 1000,
                      'parents': [{'id': p, 'displayId': p[:11]} for p in parents.split()]})
    return JSONResponse(_bb_page(request, items))


def parse_search(query: str) -> tuple[dict, list[str]]:
    """'repo:BIGDATA/hr_data ext:sql tb_log' → ({'repo': …, 'ext': …}, ['tb_log'])."""
    mods, words = {}, []
    for tok in shlex.split(query or ''):
        name, sep, val = tok.partition(':')
        if sep and name.lower() in ('repo', 'project', 'ext', 'lang', 'path'):
            mods[name.lower()] = val
        else:
            words.append(tok)
    return mods, words


async def bb_search(request: Request):
    """Поиск кода: все слова запроса в файле (git grep --all-match), как у Bitbucket — без морфологии."""
    body = await request.json()
    mods, words = parse_search(body.get('query', ''))
    limit = int((body.get('limits') or {}).get('primary', PAGE_SIZE))
    per_file = int((body.get('limits') or {}).get('secondary', 3))
    values = []
    for (key, slug), repo in _repos().items():
        if mods.get('project', key).upper() != key or mods.get('repo', f'{key}/{slug}').lower() != f'{key}/{slug}'.lower():
            continue
        if not words:
            continue
        args = ['grep', '-n', '-I', '-i', '-F', '--all-match', *sum((['-e', w] for w in words), []), 'HEAD']
        if mods.get('ext'):
            args += ['--', f'*.{mods["ext"]}']
        try:
            out = git(repo, *args)
        except FileNotFoundError:  # git grep без совпадений — код 1
            continue
        hits: dict[str, list] = {}
        for line in out.splitlines():
            _, path, num, text = line.split(':', 3)
            hits.setdefault(path, []).append({'line': int(num), 'text': html.escape(text)})
        values += [{'repository': repo_json(key, slug), 'file': path, 'hitCount': len(h), 'pathMatches': [],
                    'hitContexts': [[c] for c in h[:per_file]]} for path, h in hits.items()]
    return JSONResponse({'scope': {'type': 'GLOBAL'}, 'query': {'substituted': False},
                         'code': {'category': 'primary', 'isLastPage': len(values) <= limit, 'count': len(values),
                                  'start': 0, 'nextStart': limit, 'values': values[:limit]}})


BB_USER = {'name': 'stand', 'emailAddress': 'stand@stand.test', 'id': 1, 'displayName': USER['displayName'],
           'active': True, 'slug': 'stand', 'type': 'NORMAL'}


async def bb_users(request: Request):
    return JSONResponse(BB_USER if 'slug' in request.path_params else _bb_page(request, [BB_USER]))


async def bb_empty_page(request: Request):
    return JSONResponse(_bb_page(request, []))


async def bb_browse(request: Request):
    """Веб-ссылка /projects/<P>/repos/<r>/browse/<path> — текст файла или листинг каталога."""
    return await bb_raw(request)


# ── Сборка ───────────────────────────────────────────────────────────────────

PUBLIC_PATHS = ('/pages/viewpage.action', '/display/')


async def guard(request: Request, call_next) -> Response:
    """Bearer PAT на REST, журнал каждого запроса; 404 — сигнал, что mcp-atlassian спросил новое."""
    if request.url.path.startswith('/rest/') and TOKENS:
        auth = request.headers.get('authorization', '')
        if not (auth.startswith('Bearer ') and auth[7:] in TOKENS):
            logger.info('%s %s → 401', request.method, request.url.path)
            return JSONResponse({'message': 'Client must be authenticated to access this resource.'}, status_code=401)
    response = await call_next(request)
    level = logging.WARNING if response.status_code == 404 else logging.INFO
    logger.log(level, '%s %s?%s → %s', request.method, request.url.path, request.url.query, response.status_code)
    return response


CONFLUENCE_ROUTES = [
    Route('/rest/api/content', content_list),
    Route('/rest/api/content/search', content_search),
    Route('/rest/api/content/{id}', content_get),
    Route('/rest/api/content/{id}/child', child_any),
    Route('/rest/api/content/{id}/child/{kind}', children),
    Route('/rest/api/content/{id}/descendant/{kind}', children),
    Route('/rest/api/content/{id}/label', labels),
    Route('/rest/api/content/{id}/history', empty_list),
    Route('/rest/api/content/{id}/property', empty_list),
    Route('/rest/api/group/{name}/member', group_members),
    Route('/rest/api/search', search),
    Route('/rest/api/search/user', user_search),
    Route('/rest/api/space', spaces),
    Route('/rest/api/space/{key}', space_get),
    Route('/rest/api/user/current', current_user),
    Route('/pages/viewpage.action', viewpage),
    Route('/display/{key}/{title:path}', display),
]

JIRA_ROUTES = [
    Route('/rest/api/2/serverInfo', jira_server_info),
    Route('/rest/api/2/myself', jira_myself),
    Route('/rest/api/2/user', jira_myself),
    Route('/rest/api/2/search', jira_search, methods=['GET', 'POST']),
    Route('/rest/api/2/issue/{key}', jira_issue),
    Route('/rest/api/2/issue/{key}/comment', jira_comments),
    Route('/rest/api/2/issue/{key}/transitions', jira_transitions),
    Route('/rest/api/2/project', jira_projects),
    Route('/rest/api/2/project/{key}', jira_project),
    Route('/rest/api/2/field', jira_fields),
    Route('/rest/api/2/issueLinkType', jira_link_types),
]

def _bb_rest(api: str) -> list[Route]:
    """REST Bitbucket DC отвечает и под latest, и под 1.0: форк mcp-atlassian ходит в 1.0."""
    repo = api + '/projects/{key}/repos/{slug}'
    return [
        Route(api + '/application-properties', bb_app_props),
        Route(api + '/projects', bb_projects),
        Route(api + '/projects/{key}', bb_project),
        Route(api + '/projects/{key}/repos', bb_repos),
        Route(api + '/repos', bb_repos),
        Route(repo, bb_repo),
        Route(repo + '/raw/{path:path}', bb_raw),
        Route(repo + '/browse', bb_browse_json),
        Route(repo + '/browse/{path:path}', bb_browse_json),
        Route(repo + '/branches', bb_branches),
        Route(repo + '/commits', bb_commits),
        Route(repo + '/pull-requests', bb_empty_page),
        Route(api + '/dashboard/pull-requests', bb_empty_page),
        Route(api + '/inbox/pull-requests', bb_empty_page),
        Route(api + '/users', bb_users),
        Route(api + '/users/{slug}', bb_users),
    ]


BITBUCKET_ROUTES = [
    *_bb_rest('/rest/api/latest'),
    *_bb_rest('/rest/api/1.0'),
    Route('/rest/search/latest/search', bb_search, methods=['POST']),
    Route('/projects/{key}/repos/{slug}/browse', bb_browse),
    Route('/projects/{key}/repos/{slug}/browse/{path:path}', bb_browse),
]


async def not_found(request: Request, exc) -> JSONResponse:
    """JSON и на неизвестный путь: текстовый 404 клиент принимает за битый ответ, а не за «нет»."""
    return JSONResponse({'statusCode': 404, 'message': f'{request.url.path}: не эмулируется'}, status_code=404)


ROUTES = {'jira': JIRA_ROUTES, 'stash-delta': BITBUCKET_ROUTES, 'stash-sigma': BITBUCKET_ROUTES}
app = Starlette(routes=ROUTES.get(SERVER, CONFLUENCE_ROUTES),
                middleware=[Middleware(BaseHTTPMiddleware, dispatch=guard)],
                exception_handlers={404: not_found})


if __name__ == '__main__':
    # Самопроверка разбора запросов: `python atlassian_mock.py`.
    groups, order = parse_query('space = HRData AND label = "table" OR title ~ "ue circle" ORDER BY lastmodified DESC')
    assert groups == [[('space', '=', ['HRData']), ('label', '=', ['table'])], [('title', '~', ['ue circle'])]], groups
    assert order == 'lastmodified DESC'
    assert parse_query('key in (E360-1, E360-2)')[0] == [[('key', 'in', ['E360-1', 'E360-2'])]]
    assert _cmp('!=', 'Done', ['done']) is False and _words_in('круг общения', 'Круг общения сотрудника')
    assert parse_search('repo:BIGDATA/hr_data ext:sql "tb log" pr_x') == (
        {'repo': 'BIGDATA/hr_data', 'ext': 'sql'}, ['tb log', 'pr_x'])
    print('ok')
