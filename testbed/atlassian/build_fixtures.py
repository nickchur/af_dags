"""🧱 Наполнение эмулятора Confluence и Jira из наших репозиториев.
*2026-10-02 10:04 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Пересобирает то, что генерируется, и не трогает то, что положено руками:

    confluence/delta/ETL/      ← etl-core: docs/*.md и навыки skill/*.md
    confluence/delta/AFDAGS/   ← af_dags: readme каталогов и навыки */skill/*.md
    jira/issues.json           ← задачи HRPDATALAB-* и E360-* из истории коммитов обоих репозиториев

Корпоративные страницы (выгрузки, описания таблиц GP) кладутся руками в свои пространства
с теми же id, что в корпоративном Confluence, и живут только на стенде. Если страницы,
на которую ссылается код или навык, ещё нет, создаётся заглушка (`STUBS`): ссылка
открывается, а текст говорит, что настоящее содержимое — в корпоративном Confluence.

    python3 build_fixtures.py --etl-core ~/etl-core --af-dags ~/ctl --out ./fixtures
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import zlib
from pathlib import Path

#: Сгенерированным страницам — id за пределами корпоративного диапазона (там до ~2.5e10).
GENERATED_ID_BASE = 90_000_000_000
JIRA_KEY = re.compile(r'\b(HRPDATALAB-\d+|E360-\d+)\b')
#: (сервер, пространство, id, название) страниц, на которые ссылаются код и навыки.
STUBS = [
    ('delta', 'HRData', '1774392110', 'HR Data'),
    ('sber', 'STUB', '22973252125', 'Страница корпоративного Confluence 22973252125'),
    ('sber', 'STUB', '24078719992', 'Quality Gate выгрузок в Postgres'),
]


def page_id(rel: str) -> str:
    return str(GENERATED_ID_BASE + zlib.crc32(rel.encode()))


def title_of(text: str, fallback: str) -> str:
    m = re.search(r'^#\s+(.+)$', text, re.M)
    return re.sub(r'[*`]', '', m.group(1)).strip() if m else fallback


def write_page(path: Path, title: str, body: str, parent: str | None = None, labels=()) -> None:
    head = [f'title: {json.dumps(title, ensure_ascii=False)}']
    if parent:
        head.append(f'parent: {parent}')
    if labels:
        head.append(f'labels: {json.dumps(list(labels), ensure_ascii=False)}')
    path.write_text('---\n' + '\n'.join(head) + '\n---\n' + body, encoding='utf-8')


def build_space(out: Path, key: str, name: str, root_title: str, files: list[tuple[str, Path]]) -> int:
    """Пространство: корневая страница и по странице на файл. Названия в пространстве уникальны."""
    space = out / 'confluence' / 'delta' / key
    shutil.rmtree(space, ignore_errors=True)
    space.mkdir(parents=True)
    (space / '_space.yaml').write_text(f'name: {name}\ndescription: Сгенерировано build_fixtures.py\n',
                                       encoding='utf-8')
    root = page_id(f'{key}/')
    write_page(space / f'{root}.md', root_title, f'# {root_title}\n\nДокументы репозитория — дочерние страницы.\n')
    seen = {root_title}
    for rel, path in files:
        text = path.read_text(encoding='utf-8')
        title = title_of(text, rel)
        if title in seen:
            title = f'{title} ({rel})'
        seen.add(title)
        write_page(space / f'{page_id(key + "/" + rel)}.md', title, text, parent=root,
                   labels=['skill'] if '/skill/' in f'/{rel}' else ['doc'])
    return len(files)


def git_log(repo: Path) -> list[tuple[str, str, str]]:
    out = subprocess.run(['git', '-C', str(repo), 'log', '--no-merges', '--format=%aI%x1f%s%x1f%b%x1e'],
                         capture_output=True, text=True, check=True).stdout
    return [tuple(rec.strip('\n').split('\x1f', 2)) for rec in out.split('\x1e') if rec.strip()]


def build_jira(out: Path, repos: dict[str, Path]) -> int:
    issues: dict[str, dict] = {}
    for label, repo in repos.items():
        for date, subject, body in reversed(git_log(repo)):
            for key in set(JIRA_KEY.findall(subject + ' ' + body)):
                clean = JIRA_KEY.sub('', subject).strip(' :—-') or subject
                issue = issues.setdefault(key, {'key': key, 'summary': clean, 'description': body.strip()[:2000],
                                                'status': 'Done', 'created': date, 'labels': [], 'comments': []})
                issue['updated'] = max(issue.get('updated', date), date)
                if label not in issue['labels']:
                    issue['labels'].append(label)
                if clean != issue['summary'] and len(issue['comments']) < 20:
                    issue['comments'].append(f'{label}: {clean}')
    (out / 'jira').mkdir(parents=True, exist_ok=True)
    (out / 'jira' / 'issues.json').write_text(json.dumps(sorted(issues.values(), key=lambda i: i['key']),
                                                         ensure_ascii=False, indent=1), encoding='utf-8')
    return len(issues)


def build_stubs(out: Path) -> int:
    made = 0
    for server, key, pid, title in STUBS:
        space = out / 'confluence' / server / key
        if any(space.parent.glob(f'*/{pid}.md')):
            continue
        space.mkdir(parents=True, exist_ok=True)
        host = 'confluence.delta.sbrf.ru' if server == 'delta' else 'confluence.sberbank.ru'
        write_page(space / f'{pid}.md', title,
                   f'Заглушка стенда. Настоящая страница — '
                   f'https://{host}/pages/viewpage.action?pageId={pid}\n', labels=['stub'])
        made += 1
    return made


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--etl-core', type=Path, required=True)
    ap.add_argument('--af-dags', type=Path, required=True)
    ap.add_argument('--out', type=Path, required=True)
    a = ap.parse_args()
    etl = [(str(p.relative_to(a.etl_core)), p) for p in sorted(a.etl_core.glob('docs/*.md'))]
    etl += [(str(p.relative_to(a.etl_core)), p)
            for p in sorted(a.etl_core.glob('sber_app_dataplatform_etl_core/skill/*.md'))]
    dags = [(str(p.relative_to(a.af_dags)), p) for p in sorted(a.af_dags.glob('*/readme.md'))
            if not p.parent.name.startswith(('.', 'openspec', 'testbed'))]
    dags += [(str(p.relative_to(a.af_dags)), p) for p in sorted(a.af_dags.glob('*/skill/*.md'))]
    print('ETL:', build_space(a.out, 'ETL', 'ETL Core (стенд)', 'ETL Core', etl))
    print('AFDAGS:', build_space(a.out, 'AFDAGS', 'AF DAGs (стенд)', 'AF DAGs', dags))
    print('Jira:', build_jira(a.out, {'etl-core': a.etl_core, 'af_dags': a.af_dags}))
    print('заглушек:', build_stubs(a.out))


if __name__ == '__main__':
    main()
