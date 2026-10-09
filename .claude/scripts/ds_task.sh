#!/usr/bin/env bash
# Исполнитель задачи execute — DeepSeek через opencode: `ds_task.sh NN <модель> [fix|minor]`.
# *2026-10-09 15:59 MSK · v1.0 · Nick Churkin · NSChurkin@sber.ru*
#
# Тот же порядок, что у agy_task.sh: бриф .sberpowers/tasks/NN-brief.md (и NN-context.md, если есть),
# промпт из execute/references/implementer-prompt.md, запуск в worktree ../<репо>-dsNN от HEAD.
# Отчёт и лог (NN-ds-report.md, NN-ds.log) копируются обратно в .sberpowers/tasks/; копия остаётся
# для проверки диспетчером — коммит в ней, cherry-pick в ветку, worktree remove.
# fix — Critical/Important из NN-review-*.md, minor — хвост Minor.
#
# Модель — deepseek/deepseek-v4-pro (проверена в etl-core 1.2.9, 09.10.2026). Конфиг opencode —
# ~/.config/opencode-exec/opencode.json (edit/bash allow, webfetch deny), ключ — `secret deepseek`.
# DeepSeek НЕ коммитит: коммит делает диспетчер после механической проверки (CLAUDE.md, «execute»).
set -euo pipefail
NN=${1:?номер задачи, например 01}; MODEL=${2:?модель opencode, например deepseek/deepseek-v4-pro}; MODE=${3:-}   # fix — Critical/Important из ревью, minor — хвост Minor
MAIN=$(git rev-parse --show-toplevel); cd "$MAIN"
T=.sberpowers/tasks; BRIEF=$T/$NN-brief.md; REPORT=$T/$NN-ds-report.md
[ -f "$BRIEF" ] || { echo "нет брифа $BRIEF" >&2; exit 2; }
WT=$MAIN-ds$NN
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

echo "deepseek: задача $NN, модель $MODEL${MODE:+, $MODE} → $T/$NN-ds.log" >&2
rc=0; OPENCODE_CONFIG=$HOME/.config/opencode-exec/opencode.json OPENCODE_DISABLE_CLAUDE_CODE=1 \
  DEEPSEEK_API_KEY=$(secret deepseek) timeout 25m ~/.local/bin/opencode run --pure -m "$MODEL" "$PROMPT" \
  </dev/null >> "$T/$NN-ds.log" 2>&1 || rc=$?
cp "$T/$NN-ds"* "$MAIN/$T/"
echo "── deepseek rc=$rc · копия $WT"; tail -8 "$T/$NN-ds.log"
echo "── изменения в копии:"; git status --short; git diff --stat
