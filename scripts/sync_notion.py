#!/usr/bin/env python3
"""
Best-effort Notion sync for Quant Ideas digest.
Requires env:
- NOTION_TOKEN (or NOTION_API_KEY)
- NOTION_QUANT_IDEAS_PAGE_ID (page id of OpenClaw/Quant Ideas parent)
"""

from __future__ import annotations

import argparse
import json
import os
import textwrap
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Singapore")
NOTION_VERSION = "2022-06-28"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def notion_request(method: str, path: str, token: str, payload: dict | None = None) -> dict:
    url = f"https://api.notion.com/v1/{path.lstrip('/')}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_VERSION,
        },
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))


def chunk_text(text: str, n: int = 1800) -> list[str]:
    out = []
    i = 0
    while i < len(text):
        out.append(text[i : i + n])
        i += n
    return out


def build_children(markdown_text: str) -> list[dict]:
    blocks = []
    for part in chunk_text(markdown_text, 1500):
        blocks.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": part,
                            },
                        }
                    ]
                },
            }
        )
    return blocks[:80]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default=".")
    parser.add_argument("--markdown", required=True)
    parser.add_argument("--title", required=True)
    args = parser.parse_args()

    base = Path(args.base).expanduser().resolve()
    load_env_file(base / "state" / "notion.env")
    token = os.getenv("NOTION_TOKEN") or os.getenv("NOTION_API_KEY")
    parent_page_id = os.getenv("NOTION_QUANT_IDEAS_PAGE_ID")

    result = {
        "ok": False,
        "timestamp": datetime.now(tz=TZ).isoformat(),
        "title": args.title,
        "reason": "",
        "page_id": None,
        "url": None,
    }

    if not token:
        result["reason"] = "missing NOTION_TOKEN / NOTION_API_KEY"
    elif not parent_page_id:
        result["reason"] = "missing NOTION_QUANT_IDEAS_PAGE_ID (OpenClaw/Quant Ideas parent page id)"
    else:
        md_text = Path(args.markdown).read_text(encoding="utf-8")
        payload = {
            "parent": {"page_id": parent_page_id},
            "properties": {
                "title": {
                    "title": [
                        {
                            "type": "text",
                            "text": {"content": args.title},
                        }
                    ]
                }
            },
            "children": build_children(md_text),
        }
        try:
            response = notion_request("POST", "pages", token, payload)
            result["ok"] = True
            result["page_id"] = response.get("id")
            result["url"] = response.get("url")
            result["reason"] = "success"
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            result["reason"] = f"HTTP {e.code}: {body[:400]}"
        except Exception as e:
            result["reason"] = str(e)

    log_path = base / "logs" / f"notion-sync-{datetime.now(tz=TZ).strftime('%Y%m%d-%H%M%S')}.json"
    log_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"result": result, "log_path": str(log_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
