#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--markdown", required=True)
    p.add_argument("--json", dest="json_path", required=True)
    p.add_argument("--github", required=True)
    p.add_argument("--notion", required=True)
    p.add_argument("--feishu", required=True)
    p.add_argument("--reason", required=True)
    args = p.parse_args()

    md_path = Path(args.markdown)
    json_path = Path(args.json_path)
    github_md_path = md_path.parent.parent / "github" / md_path.name

    old = None
    text = md_path.read_text(encoding="utf-8")
    marker = "## 六、运行与同步状态\n"
    idx = text.find(marker)
    if idx == -1:
        raise SystemExit("sync status block not found")
    new_block = (
        "## 六、运行与同步状态\n"
        f"- GitHub：{args.github}\n"
        f"- Notion：{args.notion}\n"
        f"- 飞书：{args.feishu}\n"
        f"- 失败原因与重试建议：{args.reason}\n"
    )
    next_idx = text.find("\n## ", idx + len(marker))
    if next_idx == -1:
        text = text[:idx] + new_block + "\n"
    else:
        text = text[:idx] + new_block + text[next_idx + 1 :]
    md_path.write_text(text, encoding="utf-8")
    github_md_path.write_text(text, encoding="utf-8")

    data = json.loads(json_path.read_text(encoding="utf-8"))
    data["sync_status"] = {
        "github": args.github,
        "notion": args.notion,
        "feishu": args.feishu,
        "failure_reason": args.reason,
    }
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
