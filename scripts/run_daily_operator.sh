#!/bin/zsh
set -euo pipefail

BASE="/Users/yinshuo/.openclaw/workspace/quant_ideas"
cd "$BASE"

RUN_JSON=$(python3 scripts/quant_ideas_pipeline.py \
  --base "$BASE" \
  --github-status 处理中 \
  --notion-status 处理中 \
  --feishu-status 成功 \
  --failure-reason '处理中')

eval "$(python3 - <<'PY' "$RUN_JSON"
import json, shlex, sys
payload=json.loads(sys.argv[1])
r=payload['result']
md=r['markdown_path']
js=r['json_path']
dt=r['dt_file']
parts=dt.split(' - ')
title=parts[0] + ' - ' + parts[1][:2] + ':' + parts[1][2:]
print('MARKDOWN=' + shlex.quote(md))
print('JSON_PATH=' + shlex.quote(js))
print('TITLE=' + shlex.quote(title))
print('DT_FILE=' + shlex.quote(dt))
PY
)"

NOTION_OK=失败
REASON=""
if NOTION_RAW=$(python3 scripts/sync_notion.py --base "$BASE" --markdown "$MARKDOWN" --title "$TITLE" 2>/dev/null); then
  NOTION_CHECK=$(python3 - <<'PY' "$NOTION_RAW"
import json,sys
print('ok' if json.loads(sys.argv[1])['result']['ok'] else 'fail')
PY
)
  if [ "$NOTION_CHECK" = "ok" ]; then
    NOTION_OK=成功
    REASON="已完成24h时效过滤、差异化实用启发、Line内容增强与今日结论扩展。Binance RSS 若空返回则自动降级跳过，不阻断主流程。"
  else
    NOTION_OK=失败
    REASON="Notion 同步失败，详见 logs/notion-sync-*.json；GitHub 和本地日报仍已生成。"
  fi
else
  NOTION_OK=失败
  REASON="Notion 同步脚本执行失败，详见 logs/notion-sync-*.json；GitHub 和本地日报仍已生成。"
fi

python3 scripts/finalize_report.py \
  --markdown "$MARKDOWN" \
  --json "$JSON_PATH" \
  --github 成功 \
  --notion "$NOTION_OK" \
  --feishu 成功 \
  --reason "$REASON"

git add README.md prompts/improvements.md scripts/*.py scripts/*.sh state/last_run.json reports/daily reports/github reports/json export/notion export/feishu logs
if ! git diff --cached --quiet; then
  git commit -m "daily quant ideas: $(date '+%Y-%m-%d %H:%M')"
fi
GIT_SSH_COMMAND='ssh -o BatchMode=yes -o ConnectTimeout=12 -o StrictHostKeyChecking=accept-new' git push

printf 'DONE\nreport=%s\nnotion=%s\n' "$MARKDOWN" "$NOTION_OK"
