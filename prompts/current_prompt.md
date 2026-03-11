# Quant Ideas Daily Operator Prompt (active)

## Goal
Build and run a daily quant research digest pipeline with two-phase governance:
- Phase A: one-shot implementation + full-chain acceptance
- Phase B: enable daily scheduler only after Phase A passes

## Scope
- Quant research (microstructure, alpha, ML/RL, crypto structure, execution, tooling)
- Markets line (macro/liquidity, cross-asset news, exchange updates, event-driven themes)

## Output policy
- Keep only score 4-5 in main body
- Score 3 goes to backup
- Merge duplicates
- Prefer fewer high-quality items over noisy coverage

## Required output sections
1. Metadata
2. 今日最值得关注（<=7）
3. Research Line
4. Markets Line
5. 备选阅读（3/5）
6. 今日结论（3+3+3）
7. 运行与同步状态

## Sync policy
- GitHub push required
- Notion sync required (or clear failure report + exported draft)
- Feishu sync required (or failure report + safe retry + exported draft)

## Timezone
Asia/Singapore
