#!/usr/bin/env bash
# Исполнитель задачи execute — agy вместо субагента: `agy_task.sh NN <модель> [fix]`.
# *2026-10-04 11:23 MSK · v1.1 · Nick Churkin · NSChurkin@sber.ru*
#
# Берёт бриф .sberpowers/tasks/NN-brief.md (и NN-context.md, если диспетчер его написал),
# собирает промпт из шаблона навыка execute/references/implementer-prompt.md и запускает agy
# в корне репозитория без подтверждений. Отчёт agy пишет в NN-report.md, лог — NN-agy.log.
# С третьим аргументом `fix` agy чинит находки ревью из NN-review-*.md и дописывает отчёт.
#
# agy НЕ коммитит: коммит делает диспетчер после механической проверки (CLAUDE.md,
# «execute: исполнитель agy»). Модели — `agy models`; ключ — секрет agy в Bitwarden.
set -euo pipefail
NN=${1:?номер задачи, например 01}; MODEL=${2:?модель agy, например gemini-3.1-pro-high}; MODE=${3:-}
cd "$(git rev-parse --show-toplevel)"
T=.sberpowers/tasks; BRIEF=$T/$NN-brief.md; REPORT=$T/$NN-report.md
[ -f "$BRIEF" ] || { echo "нет брифа $BRIEF" >&2; exit 2; }
S=$(secret agy) || exit 1; eval "$S"; export GEMINI_API_KEY; unset S

TITLE=$(sed -n '1s/^#* *//p' "$BRIEF")
CONTEXT=$( [ -f "$T/$NN-context.md" ] && cat "$T/$NN-context.md" || echo "Контекста сверх брифа нет.")
# тело шаблона — первый блок ``` … ``` после «## Промпт», без строк-обёртки «Субагент:»
TEMPLATE=$(awk '/^## Промпт/{p=1} p&&/^```/{n++; next} p&&n==1' .claude/skills/execute/references/implementer-prompt.md |
  sed -n '/промпт: |/,$p' | sed '1d; s/^    //')
PROMPT=${TEMPLATE//\[N\]/$NN}; PROMPT=${PROMPT//\[НАЗВАНИЕ\]/$TITLE}
PROMPT=${PROMPT//\[BRIEF_FILE\]/$BRIEF}; PROMPT=${PROMPT//\[REPORT_FILE\]/$REPORT}
PROMPT=${PROMPT//\[КАТАЛОГ\]/$PWD}; PROMPT=${PROMPT//\[КОНТЕКСТ\]/$CONTEXT}

PROMPT+="

## Отличия этого запуска (важнее шаблона выше)

- Ты работаешь без человека: вопросы не задавай — неясность опиши в отчёте и верни
  NEEDS_CONTEXT или BLOCKED. Ответить тебе некому.
- НЕ делай git commit, git add, git stash, git checkout, git reset — коммитит диспетчер.
- Меняй только файлы, названные в брифе. Чужие файлы не трогай, даже если видишь в них ошибку
  (опиши её в отчёте).
- Ничего не удаляй «как лишнее»: удаление, не заданное брифом, — дефект.
- Навыки упоминаются как «скилл X» — это файлы .claude/skills/X/SKILL.md; если навыка нет,
  пропусти этот пункт.
- Финальное сообщение — ровно тот статус ≤15 строк, что описан выше."
if [ "$MODE" = fix ]; then
  PROMPT+="

## Это fix-проход

Ревью нашло проблемы: прочитай $(ls $T/$NN-review-*.md | grep -v package | tr '\n' ' ')— исправь находки
Critical и Important (Minor не трогай), затем допиши в $REPORT раздел «Fix-проход»: что
исправлено по каждой находке и чем проверено."
fi

echo "agy: задача $NN, модель $MODEL${MODE:+, $MODE} → $T/$NN-agy.log" >&2
timeout 25m agy -p "$PROMPT" --model "$MODEL" --dangerously-skip-permissions --print-timeout 20m \
  > >(tee -a "$T/$NN-agy.log") 2>&1
