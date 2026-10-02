"""📥 HTML-экспорт пространства Confluence → страницы эмулятора.
*2026-10-02 10:53 MSK · v1.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Штатный экспорт Confluence в HTML (каталог с `index.html`, `toc.html`, `assets/`, по файлу на
страницу) раскладывается в `confluence/<server>/<SPACE>/<id>.html` с теми же id, названиями и
деревом, что в корпоративном Confluence. Тогда настоящие ссылки из навыков находят страницы
и на стенде. Содержимое корпоративное: результат кладётся только на стенд, в git — никогда.

    python3 import_html_export.py <каталог экспорта> --server sber --space HRTECH \\
        --out /opt/aftest/atlassian-mock/fixtures

Id и название страницы берутся из `<h1 id="src-<id>">`, дерево — из `toc.html`, корень —
страница, на которую ведёт `index.html`; его родитель — `--parent` (выгружают обычно ветку,
а не всё пространство: у ветки ПКАП1080 в HRData родитель — домашняя «HR Data»). Картинок в экспорте нет —
вместо них остаётся подпись `[изображение]`.
"""
from __future__ import annotations

import argparse
import html
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote

H1 = re.compile(r'<h1 id="src-(\d+)">\s*<span>(.*?)</span>', re.S)
CONTENT = re.compile(r'<div id="main-content"[^>]*>(.*)</div>\s*</article>', re.S)
IMG = re.compile(r'<img\b[^>]*>')


class TocParser(HTMLParser):
    """Родитель каждой страницы по вложенности `<ul>/<li>` оглавления."""

    def __init__(self) -> None:
        super().__init__()
        self.stack: list[str | None] = [None]  # id страницы, чей список сейчас открыт
        self.last: str | None = None
        self.parent: dict[str, str | None] = {}

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'ul':
            self.stack.append(self.last)
        elif tag == 'a' and a.get('data-destpageid'):
            self.last = a['data-destpageid']
            self.parent[self.last] = self.stack[-1]

    def handle_endtag(self, tag):
        if tag == 'ul' and len(self.stack) > 1:
            self.stack.pop()


def parse_page(text: str) -> tuple[str, str, str] | None:
    head, body = H1.search(text), CONTENT.search(text)
    if not head or not body:
        return None
    return head.group(1), html.unescape(head.group(2)).strip(), IMG.sub('[изображение]', body.group(1)).strip()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('export', type=Path)
    ap.add_argument('--server', required=True, help='delta | sber')
    ap.add_argument('--space', required=True, help='ключ пространства, как в корпоративном Confluence')
    ap.add_argument('--name', help='название пространства (по умолчанию — из заголовка страниц)')
    ap.add_argument('--parent', help='id родителя корневой страницы, если выгружена ветка пространства')
    ap.add_argument('--out', type=Path, required=True)
    a = ap.parse_args()

    toc = TocParser()
    toc.feed((a.export / 'toc.html').read_text(encoding='utf-8'))
    start = re.search(r'URL=([^"]+)"', (a.export / 'index.html').read_text(encoding='utf-8'))
    root_file = unquote(start.group(1)) if start else None

    space = a.out / 'confluence' / a.server / a.space
    space.mkdir(parents=True, exist_ok=True)
    pages, root_id, space_name = {}, None, a.name
    for f in sorted(a.export.glob('*.html')):
        page = parse_page(f.read_text(encoding='utf-8'))
        if not page:  # index, toc, search
            continue
        pid, title, body = page
        pages[pid] = (title, body)
        if f.name == root_file:
            root_id = pid
        if not space_name:
            m = re.search(r'<title>.*? - (.*?)</title>', f.read_text(encoding='utf-8'), re.S)
            space_name = html.unescape(m.group(1)).strip() if m else a.space
    for pid, (title, body) in pages.items():
        parent = a.parent if pid == root_id else toc.parent.get(pid) or root_id
        head = [f'title: {json.dumps(title, ensure_ascii=False)}', 'labels: ["export"]']
        if parent and (parent in pages or parent == a.parent):
            head.insert(1, f'parent: {parent}')
        (space / f'{pid}.html').write_text('---\n' + '\n'.join(head) + '\n---\n' + body + '\n', encoding='utf-8')
    if not (space / '_space.yaml').exists():
        (space / '_space.yaml').write_text(f'name: {json.dumps(space_name, ensure_ascii=False)}\n'
                                           f'description: HTML-экспорт корпоративного Confluence\n',
                                           encoding='utf-8')
    print(f'{a.server}/{a.space}: страниц {len(pages)}, корень {root_id}')


if __name__ == '__main__':
    main()
