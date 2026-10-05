#!/usr/bin/env python3
"""Сверка baseline-спек SberPowers: формат и полнота перевода из OpenSpec.
*2026-10-04 12:05 MSK · v1.1 · Nick Churkin · NSChurkin@sber.ru*

    check_baseline.py ctl-worker tools …   # перевод: openspec/specs/<cap>/spec.md → baseline
    check_baseline.py                      # формат всех docs/sberpowers/specs/*-baseline.md

Формат: REQ-ID вида REQ-<cap>-NN уникальны, идут по порядку, у каждого есть строка в
«Критериях приёмки» и в Traceability, все разделы шаблона на месте.

Перевод (пока жива старая спека): число требований совпадает; REQ-NN начинается с
названия N-го `### Requirement:` (`**REQ-…** — **<название>.**`); сценариев приёмки у
требования не меньше, чем было `#### Scenario:`; каждая строка «Происхождения» перенесена
дословно — и «откуда», и «когда пересматривать». Смысл формулировок проверяет ревьюер,
скрипт ловит то, что исполнитель потерял или выдумал.

Заодно ловит признаки текста, собранного скриптом (пустое «действие» в сценариях,
«дано —» у большинства строк), и внешние упоминания: документ уезжает в корпоративный
репозиторий.

Выход: 0 — всё сходится, 1 — есть расхождения.
"""
from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SPECS = REPO / 'docs' / 'sberpowers' / 'specs'
SECTIONS = ('## Назначение', '## Требования', '## Ограничения и контракты', '## Non-goals',
            '## Критерии приёмки', '## Traceability')


def norm(text: str) -> str:
    """Сравнение без оглядки на переносы, лишние пробелы и обрамляющие кавычки разметки."""
    return re.sub(r'\s+', ' ', text.replace('**', '')).strip()


def section(text: str, title: str) -> str:
    """Тело раздела `## title` до следующего раздела того же уровня."""
    m = re.search(rf'^{re.escape(title)}\s*$(.*?)(?=^## |\Z)', text, re.M | re.S)
    return m.group(1) if m else ''


def table_ids(body: str, cap: str) -> list[str]:
    return re.findall(rf'^\|\s*(REQ-{re.escape(cap)}-\d+)\s*\|', body, re.M)


def check_format(path: Path, cap: str) -> tuple[list[str], list[tuple[str, str]]]:
    """Замечания по формату и список (REQ-ID, строка требования)."""
    text = path.read_text(encoding='utf-8')
    problems = [f"нет раздела «{s[3:]}»" for s in SECTIONS if not re.search(rf'^{re.escape(s)}\s*$', text, re.M)]
    reqs = re.findall(rf'^- \*\*(REQ-{re.escape(cap)}-(\d+))\*\* — (.+)$', section(text, '## Требования'), re.M)
    ids = [r[0] for r in reqs]
    for rid, n in Counter(ids).items():
        if n > 1:
            problems.append(f"{rid} встречается {n} раза")
    expected = [f'REQ-{cap}-{i:02d}' for i in range(1, len(ids) + 1)]
    if ids != expected:
        problems.append(f"нумерация REQ не подряд с 01: {', '.join(ids[:5])}…")
    for name, title in (('Критерии приёмки', '## Критерии приёмки'), ('Traceability', '## Traceability')):
        missing = sorted(set(ids) - set(table_ids(section(text, title), cap)))
        if missing:
            problems.append(f"в «{name}» нет {', '.join(missing)}")
    if not ids:
        problems.append(f"нет ни одного требования вида **REQ-{cap}-NN**")
    # признаки текста, собранного скриптом по шаблону: пустое «действие» в сценариях
    # (так выглядели оба отклонённых прохода 04.10.2026), «дано —» у большинства строк
    rows = re.findall(r'^\|\s*REQ-.*$', section(text, '## Критерии приёмки'), re.M)
    if (n := sum('действие —' in r for r in rows)):
        problems.append(f"«действие —» в {n} сценариях: действие есть у каждого сценария")
    if rows and (n := sum('дано —' in r for r in rows)) > len(rows) // 3:
        problems.append(f"«дано —» в {n} из {len(rows)} сценариев: предусловие вынесено в действие?")
    # документ уезжает в корпоративный репозиторий
    for word in sorted(set(re.findall(r'PR #\d+|\b(?:Claude|GitHub|Gemini|agy)\b', text, re.I))):
        problems.append(f"внешнее упоминание «{word}»")
    return problems, [(r[0], r[2]) for r in reqs]


def check_translation(cap: str) -> list[str]:
    old = (REPO / 'openspec' / 'specs' / cap / 'spec.md').read_text(encoding='utf-8')
    path = SPECS / f'{cap}-baseline.md'
    if not path.exists():
        return [f"нет {path.relative_to(REPO)}"]
    problems, reqs = check_format(path, cap)
    new = path.read_text(encoding='utf-8')

    blocks = re.split(r'^### Requirement:\s*', old, flags=re.M)[1:]
    titles = [b.split('\n', 1)[0].strip() for b in blocks]
    scenarios = [len(re.findall(r'^#### Scenario:', b, re.M)) for b in blocks]
    if len(reqs) != len(titles):
        problems.append(f"требований было {len(titles)}, стало {len(reqs)}")
    accept = Counter(table_ids(section(new, '## Критерии приёмки'), cap))
    for i, (title, want) in enumerate(zip(titles, scenarios)):
        if i >= len(reqs):
            break
        rid, line = reqs[i]
        if not norm(line).startswith(norm(title)):
            problems.append(f"{rid} не начинается с названия «{title}»")
        if accept[rid] < want:
            problems.append(f"{rid}: сценариев было {want}, в «Критериях приёмки» {accept[rid]}")

    flat = norm(new)
    for row in re.findall(r'^\|(?!\s*-)(.+)\|\s*$', section(old, '## Происхождение'), re.M):
        cells = [c.strip() for c in row.split('|')]
        if len(cells) < 3 or cells[0] == 'Требование':
            continue
        for label, cell in (('откуда', cells[1]), ('когда пересматривать', cells[2])):
            if norm(cell) not in flat:
                problems.append(f"«Происхождение» «{cells[0][:50]}»: потеряно «{label}»")
    return problems


def main() -> int:
    caps = sys.argv[1:]
    report: dict[str, list[str]] = {}
    if caps:
        for cap in caps:
            report[cap] = check_translation(cap)
    else:
        for path in sorted(SPECS.glob('*-baseline.md')):
            cap = path.name.removesuffix('-baseline.md')
            report[cap] = check_format(path, cap)[0]
    bad = 0
    for cap, problems in report.items():
        print(f"{'✅' if not problems else '❌'} {cap}" + (f": {len(problems)}" if problems else ''))
        for p in problems:
            print(f"   • {p}")
        bad += bool(problems)
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
