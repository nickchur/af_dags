#!/usr/bin/env bash
# Исполнитель задачи execute — agy вместо субагента: `agy_task.sh NN <модель> [fix]`.
# *2026-10-04 12:06 MSK · v2.1 · Nick Churkin · NSChurkin@sber.ru*
#
# Берёт бриф .sberpowers/tasks/NN-brief.md (и NN-context.md, если диспетчер его написал),
# собирает промпт из шаблона навыка execute/references/implementer-prompt.md и запускает agy
# без подтверждений в своей копии репозитория — worktree ../<репо>-wtNN от текущего HEAD:
# параллельные задачи не видят друг друга, а git status копии показывает ровно работу задачи.
# Отчёт и лог (NN-report.md, NN-agy.log) копируются обратно в .sberpowers/tasks/; копия
# остаётся для проверки диспетчером — коммит в ней, cherry-pick в ветку, worktree remove.
# С третьим аргументом `fix` agy чинит находки ревью из NN-review-*.md и дописывает отчёт.
#
# agy НЕ коммитит: коммит делает диспетчер после механической проверки (CLAUDE.md,
# «execute: исполнитель agy»). Модели — `agy models`. Ключ не передаётся: у agy своя
# авторизация в ~/.gemini (параллельные запросы ключа к Bitwarden падали).
set -euo pipefail
NN=${1:?номер задачи, например 01}; MODEL=${2:?модель agy, например gemini-3.8-flash-high}; MODE=${3:-}   # fix — Critical/Important из ревью, minor — хвост Minor
MAIN=$(git rev-parse --show-toplevel); cd "$MAIN"
T=.sberpowers/tasks; BRIEF=$T/$NN-brief.md; REPORT=$T/$NN-report.md
[ -f "$BRIEF" ] || { echo "нет брифа $BRIEF" >&2; exit 2; }
WT=$MAIN-wt$NN
[ -d "$WT" ] && { echo "копия $WT уже есть — проверь и убери: git worktree remove --force $WT" >&2; exit 2; }
git worktree add -q --detach "$WT" HEAD && cp -r .sberpowers "$WT/" && cd "$WT"

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
- Вспомогательных файлов (скриптов, черновиков) в репозитории не создавай: нужна заготовка —
  только в /tmp. Текст документов пиши сам, а не генерируй скриптом с заменами по шаблону:
  механическая замена ломает грамматику и смысл, а проверка формы этого не видит.
- Навыки упоминаются как «скилл X» — это файлы .claude/skills/X/SKILL.md; если навыка нет,
  пропусти этот пункт.
- Финальное сообщение — ровно тот статус ≤15 строк, что описан выше."
if [ "$MODE" = fix ]; then
  PROMPT+="

## Это fix-проход

Ревью нашло проблемы: прочитай $(ls $T/$NN-review-*.md | grep -v package | tr '\n' ' ')— исправь находки
Critical и Important (Minor не трогай), затем допиши в $REPORT раздел «Fix-проход»: что
исправлено по каждой находке и чем проверено."
elif [ "$MODE" = minor ]; then
  PROMPT+="

## Это проход по Minor

Все раунды ревью приняты; остались замечания Minor: прочитай $(ls $T/$NN-review-*.md | grep -v package | tr '\n' ' ')—
исправь в файле задачи все Minor, кроме тех, что ревьюер отнёс к исходнику («так в исходнике»,
«вопрос к следующей дельте») или пометил «требование плана». Смысл сверяй с исходником, ничего не
выдумывай. Затем допиши в $REPORT раздел «Проход по Minor»: что исправлено и что оставлено с причиной."
fi

echo "agy: задача $NN, модель $MODEL${MODE:+, $MODE} → $T/$NN-agy.log" >&2
rc=0; timeout 25m agy -p "$PROMPT" --model "$MODEL" --dangerously-skip-permissions --print-timeout 20m \
  >> "$T/$NN-agy.log" 2>&1 || rc=$?
cp "$T/$NN-"* "$MAIN/$T/"
echo "── agy rc=$rc · копия $WT"; tail -8 "$T/$NN-agy.log"
echo "── изменения в копии:"; git status --short; git diff --stat
