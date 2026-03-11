#!/usr/bin/env python3
"""
Quant Ideas Daily Operator - Stage A implementation

- Collects research + market signals from RSS/GitHub
- Deduplicates + scores
- Generates markdown digest + structured JSON
- Stores export drafts for Notion/Feishu
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import textwrap
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Singapore")
USER_AGENT = "Mozilla/5.0 (OpenClaw Quant Ideas Operator)"


@dataclass
class Item:
    title: str
    kind: str
    source: str
    published_at: str
    link: str
    summary: str
    line: str
    raw_score: int = 1
    score: int = 1
    topic: str = ""


RESEARCH_KEYWORDS = [
    "microstructure",
    "order book",
    "order flow",
    "alpha",
    "feature",
    "signal",
    "statistical arbitrage",
    "execution",
    "transaction cost",
    "reinforcement learning",
    "imitation learning",
    "online learning",
    "time series",
    "transformer",
    "crypto",
    "perpetual",
    "benchmark",
    "quant",
]

MARKET_KEYWORDS = [
    "liquidity",
    "inflation",
    "rate",
    "yield",
    "futures",
    "options",
    "volatility",
    "exchange",
    "listing",
    "fee",
    "macro",
    "policy",
    "fx",
    "crypto",
    "index",
    "bond",
]

RESEARCH_TOPICS = {
    "Microstructure / Order Flow": ["microstructure", "order book", "order flow", "fill", "execution", "latency"],
    "Alpha / Features / Signals": ["alpha", "factor", "feature", "signal", "label", "prediction"],
    "ML / RL / Imitation Learning": ["machine learning", "reinforcement", "imitation", "transformer", "neural", "online learning"],
    "Crypto / Market Structure": ["crypto", "perpetual", "on-chain", "exchange", "market structure"],
    "Tools / Repos / Research Infrastructure": ["github", "tool", "benchmark", "framework", "backtest", "infra", "simulator"],
}

MARKET_TOPICS = {
    "Macro / Liquidity": ["macro", "liquidity", "inflation", "rate", "yield", "fed", "ecb", "boj"],
    "Equities / Futures / FX / Crypto": ["equities", "stocks", "futures", "fx", "crypto", "bitcoin", "ethereum", "index"],
    "Exchange / Venue Updates": ["exchange", "listing", "fee", "contract", "venue", "announcement"],
    "Theme / Catalyst / Event-driven": ["theme", "catalyst", "event", "earnings", "policy", "geopolitical"],
}


def now_sg() -> datetime:
    return datetime.now(tz=TZ)


def ensure_dirs(base: Path) -> None:
    for rel in [
        "reports/daily",
        "reports/json",
        "reports/github",
        "cache",
        "logs",
        "prompts",
        "state",
        "export/notion",
        "export/feishu",
        "scripts",
    ]:
        (base / rel).mkdir(parents=True, exist_ok=True)


def read_url(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def cache_fetch(base: Path, source_name: str, url: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", source_name.lower()).strip("-")
    cache_path = base / "cache" / f"{slug}.xml"
    text = read_url(url)
    cache_path.write_text(text, encoding="utf-8")
    return text


def parse_rss_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(tz=timezone.utc)
    value = value.strip()
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return datetime.now(tz=timezone.utc)


def normalize_text(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def parse_rss_items(xml_text: str, source: str, kind: str, line: str) -> list[Item]:
    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    if channel is None:
        # Atom fallback
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = []
        for entry in root.findall("atom:entry", ns):
            title = normalize_text(entry.findtext("atom:title", default="", namespaces=ns))
            link_el = entry.find("atom:link", ns)
            link = link_el.attrib.get("href", "") if link_el is not None else ""
            summary = normalize_text(entry.findtext("atom:summary", default="", namespaces=ns))
            published_raw = entry.findtext("atom:updated", default="", namespaces=ns)
            published = parse_rss_datetime(published_raw).astimezone(TZ)
            if title and link:
                items.append(
                    Item(
                        title=title,
                        kind=kind,
                        source=source,
                        published_at=published.isoformat(),
                        link=link,
                        summary=summary,
                        line=line,
                    )
                )
        return items

    out: list[Item] = []
    for item in channel.findall("item"):
        title = normalize_text(item.findtext("title", default=""))
        link = normalize_text(item.findtext("link", default=""))
        summary = normalize_text(item.findtext("description", default=""))
        published_raw = item.findtext("pubDate") or item.findtext("published") or item.findtext("dc:date")
        published = parse_rss_datetime(published_raw).astimezone(TZ)
        if title and link:
            out.append(
                Item(
                    title=title,
                    kind=kind,
                    source=source,
                    published_at=published.isoformat(),
                    link=link,
                    summary=summary,
                    line=line,
                )
            )
    return out


def fetch_github_items() -> list[Item]:
    queries = [
        "quant trading order book language:Python pushed:>=2026-03-01",
        "market microstructure simulator language:Python pushed:>=2026-03-01",
        "reinforcement learning trading language:Python pushed:>=2026-03-01",
        "crypto market making language:Python pushed:>=2026-03-01",
    ]
    items: list[Item] = []
    for q in queries:
        url = (
            "https://api.github.com/search/repositories?q="
            + urllib.parse.quote(q)
            + "&sort=updated&order=desc&per_page=6"
        )
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
        for repo in payload.get("items", []):
            summary = normalize_text(repo.get("description") or "")
            stars = repo.get("stargazers_count", 0)
            summary = f"{summary} (⭐ {stars})"
            published = parse_rss_datetime(repo.get("pushed_at")).astimezone(TZ)
            items.append(
                Item(
                    title=repo.get("full_name", ""),
                    kind="GitHub",
                    source="GitHub Search API",
                    published_at=published.isoformat(),
                    link=repo.get("html_url", ""),
                    summary=summary,
                    line="research",
                )
            )
    return items


def dedup(items: list[Item]) -> list[Item]:
    seen: set[str] = set()
    out: list[Item] = []
    for it in items:
        key = re.sub(r"[^a-z0-9]+", "", it.title.lower())[:120]
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def score_item(item: Item, now: datetime) -> int:
    text = f"{item.title} {item.summary}".lower()
    score = 1

    # Relevance scoring
    score += sum(1 for k in RESEARCH_KEYWORDS if k in text) // 3
    score += sum(1 for k in MARKET_KEYWORDS if k in text) // 4

    if item.kind == "论文":
        score += 1
    if item.kind == "GitHub":
        score += 1

    try:
        published = datetime.fromisoformat(item.published_at)
        age_hours = (now - published).total_seconds() / 3600
        if age_hours <= 48:
            score += 1
        elif age_hours <= 120:
            score += 0
        else:
            score -= 1
    except Exception:
        pass

    if any(k in text for k in ["order book", "microstructure", "execution", "alpha", "reinforcement", "liquidity", "fee"]):
        score += 1

    return max(1, min(5, score))


def pick_topic(text: str, mapping: dict[str, list[str]], default_topic: str) -> str:
    t = text.lower()
    for topic, kws in mapping.items():
        if any(k in t for k in kws):
            return topic
    return default_topic


def clean_summary(summary: str, max_len: int = 280) -> str:
    summary = normalize_text(summary)
    if not summary:
        return "原文摘要较短，建议查看原文获取完整细节。"
    if len(summary) > max_len:
        return summary[: max_len - 1] + "…"
    return summary


def to_short_date(iso_text: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_text)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return iso_text[:10]


def implication_for_market(item: Item) -> str:
    t = f"{item.title} {item.summary}".lower()
    if any(k in t for k in ["liquidity", "rate", "yield", "inflation", "macro"]):
        return "关注流动性与利率预期变化对跨资产相关性与波动率的传导。"
    if any(k in t for k in ["fee", "listing", "exchange", "contract", "venue"]):
        return "关注交易成本与可交易标的变化对成交质量和策略容量的影响。"
    if any(k in t for k in ["crypto", "bitcoin", "ethereum", "perpetual"]):
        return "关注加密现货-永续基差、资金费率与风险偏好联动。"
    return "可作为事件驱动观察点，评估对成交活跃度与风格轮动的影响。"


def inspiration(item: Item) -> dict[str, str]:
    t = f"{item.title} {item.summary}".lower()
    title = item.title

    hypothesis = "可围绕“微结构变化 → 成交冲击 → 短周期收益/回撤”建立可检验假设，优先验证在高波动时段与低流动性时段是否显著。"
    model_path = "先用可解释的线性/树模型做基线，再叠加轻量时序模型（TCN/Transformer）；评估指标同时覆盖方向准确率、冲击成本与尾部回撤。"
    data_watch = "数据面建议并行跟踪逐笔成交、L2盘口、资金费率、未平仓量与交易所规则变更，并统一成事件标签 + 连续特征两套输入。"
    execution = "执行层建议先做离线仿真与回放，确认容量/滑点边界后再灰度实盘；实盘阶段用分时段参与率与冲击阈值双重约束控制风险。"

    if "slippage-at-risk" in t or ("liquidity risk" in t and "perpetual" in t):
        hypothesis = "把 SaR 指标与盘口深度、清算链路和做市商集中度联动，检验它是否能提前预警‘滑点放大 → 连锁平仓 → 流动性抽离’这一压力链条。"
        model_path = "先做简单阈值与分位数预警模型，再尝试把 SaR 作为时变风险因子并入执行模型，用来动态收缩仓位与参与率。"
        data_watch = "重点监控多档盘口深度、资金费率、爆仓数据、未平仓量和保险基金变化，形成实时压力面板。"
        execution = "实盘上可把 SaR 直接接入下单前风控，在 SaR 抬升时降低单笔规模、放宽执行周期并限制在薄深度时段加仓。"
    elif "algoxpert" in t or "overfitting" in t:
        hypothesis = "把“参数稳定区间”替代“单点最优参数”作为研究验收标准，检验策略在滚动窗口与 regime 切换下是否仍保持收益/回撤比稳定。"
        model_path = "研究流程上应引入 IS / WFA / OOS 三阶段门槛，并把 purge gap、catastrophic veto 与参数锁定写入统一模板。"
        data_watch = "除常规收益指标外，应同步看参数漂移、窗口间表现衰减、换目标函数后的排序变化，识别伪稳健策略。"
        execution = "真正落地时不要直接上最优回测版本，而是优先上线通过稳定区间检验、且对交易成本不敏感的保守参数集。"
    elif "dex" in t and "dynamic fees" in t:
        hypothesis = "把 DEX 费率变动视作微结构状态变量，检验费率切换是否会系统性改变订单流去向、套利活跃度与滑点分布。"
        model_path = "可先建立分状态回归或 regime-switching 模型，把费率、池子深度与波动水平联合解释成交质量。"
        data_watch = "重点跟踪不同池子的费率、深度、成交量、价格偏离与套利活动，观察高活跃和低活跃环境下的差异。"
        execution = "若用于交易路由，优先在仿真中测试‘低费率优先’与‘低滑点优先’两种路由规则，避免只看名义手续费。"
    elif "uncertainty quantification" in t or "selective prediction" in t:
        hypothesis = "把 selective prediction 思路迁移到信号闸门：当模型置信度不足时主动放弃交易，检验是否能显著改善胜率和尾部回撤。"
        model_path = "建模上可在原有预测器外包一层置信度/覆盖率控制模块，用统一阈值管理‘出手/不出手’决策。"
        data_watch = "重点跟踪各类置信边界、拒绝率、覆盖率和放弃交易后的绩效变化，而不是只看保留下来的命中率。"
        execution = "更适合先在纸面交易或低频策略里验证；若拒绝机制有效，再推广到高频信号筛选与自动化上线闸门。"
    elif "adaptive llm decoding" in t:
        hypothesis = "可借鉴其‘按状态分配预算’思想，把算力/延迟预算动态分配到不同研究模块上，检验是否能在固定资源下提升实验效率。"
        model_path = "不是直接预测价格，而是做一个‘研究调度器’：简单任务走轻模型，高不确定任务自动切换到更重的推理链路。"
        data_watch = "建议跟踪不同任务类型下的耗时、正确率、失败率与重试成本，形成统一的算力分配报表。"
        execution = "先用于研究基础设施与实验编排，而不是直接用于下单；确认节省资源且不降质量后，再纳入日常研究流水线。"
    elif "reinforcement" in t or "imitation" in t:
        hypothesis = "把多阶段决策、库存风险和交易成本一起写入奖励函数，检验 RL/模仿学习在执行控制问题上是否优于静态规则。"
        model_path = "可采用‘行为克隆预训练 + 离线RL微调’两阶段路线，用历史成交回放降低探索风险。"
        data_watch = "重点观察库存路径、成交率、滑点和尾部风险是否同步改善，避免只看单一收益指标。"
        execution = "先做离线策略评估和保守回放，再在小资金、低杠杆环境中灰度验证策略是否稳定。"
    elif "order book" in t or "microstructure" in t:
        hypothesis = "优先验证盘口斜率、队列位置变化、撤单密度等微结构变量对未来5-30分钟价格路径与成交质量的解释力，并测试在不同交易时段的稳定性。"
        model_path = "先做多层盘口特征的线性/树模型基线，再叠加时序卷积或轻量 Transformer 检查是否存在稳定增益。"
        data_watch = "建议记录盘口深度曲线、主动/被动成交占比、撤单率和价差变化，形成可回放的微结构样本库。"
        execution = "更适合先做分品种、分时段的小范围实验，确认微结构信号不是由个别异常时段驱动后再扩大样本。"
    elif item.kind == "GitHub":
        hypothesis = f"可直接围绕《{title}》提供的模块化能力搭建原型，检验它是否能显著缩短从研究想法到可运行实验的时间。"
        model_path = "先复现仓库自带示例，再把其中的数据接口、执行模块或评估模块替换成你的真实口径，避免一次性重写全部流程。"
        data_watch = "重点看代码结构是否利于接入你现有数据、是否具备回放/仿真能力，以及是否便于追加日志和评估指标。"
        execution = "如果样例可跑通，优先把它作为研究脚手架而不是生产系统；确认稳定后再逐步替换为你自己的模块。"

    return {
        "hypothesis": hypothesis,
        "model_path": model_path,
        "data_watch": data_watch,
        "execution": execution,
    }


def chinese_core_summary(item: Item) -> str:
    t = f"{item.title} {item.summary}".lower()
    if "slippage-at-risk" in t or ("liquidity risk" in t and "perpetual" in t):
        return "这篇文章提出 SaR（Slippage-at-Risk）框架，用当前订单簿微结构而不是历史收益分布，前瞻性地衡量永续合约市场的滑点与流动性尾部风险。它把滑点分位数、尾部期望滑点和总尾部滑点统一到一个实时风险口径里，并进一步考虑做市商集中度对市场脆弱性的放大作用。对量化研究来说，这相当于把盘口深度、清算压力、保险基金约束和执行冲击放进同一个观测框架，适合直接做交易风险监控与清算事件预警实验。"
    if "algoxpert" in t or "overfitting" in t:
        return "这篇文章聚焦量化策略从回测走向实盘时最常见的过拟合问题，提出了 IS / WFA / OOS 三段式研究框架。核心思想不是追求单点最优参数，而是寻找稳定参数区域，再通过滚动窗口、purge gap、参数锁定和风险护栏来检验策略是否经得住真实市场阶段切换。它对研究流程的直接价值很高，因为可以把‘研究规范’本身制度化，减少参数漂移、目标函数切换和事后调参带来的伪优势。"
    if "dynamic fees" in t and "dex" in t:
        return "这篇文章研究去中心化交易所之间围绕订单流展开的动态费率博弈，说明不同 DEX 会在吸引噪音交易与抑制套利之间切换定价策略。结果表明，竞争加剧通常会降低策略交易者的执行滑点，并改变噪音交易者在不同活跃度环境下的成交成本。对加密微结构研究来说，这给了一个很好的事件变量：费率机制变化本身就可能重塑订单流分配和执行质量。"
    if "uncertainty quantification" in t or "selective prediction" in t:
        return "这篇内容讨论的是不确定性量化与 selective prediction，重点在于如何在样本有限时给模型输出建立更稳健的风险控制边界。它的量化启发不在直接做交易预测，而在于把置信区间、风险保证和拒绝输出机制迁移到信号筛选、模型上线闸门和自动化研究流程中。换句话说，它更像是一个‘什么时候不该出手’的决策框架。"
    if "adaptive llm decoding" in t:
        return "这篇文章研究的是根据任务难度和剩余预算动态调整推理/采样策略，而不是固定 temperature 或 top-p。迁移到量化场景里，它对应的是‘按状态动态分配算力与决策带宽’：在高不确定阶段增加探索，在高置信阶段压缩计算和延迟。它更偏研究基础设施与在线决策调度，而不是直接生成 alpha。"
    if "reinforcement" in t or "imitation" in t:
        return "这篇内容围绕强化学习/模仿学习展开，核心价值在于把多阶段决策、延迟反馈和动态约束联合建模。放到交易场景里，更适合做执行策略、库存控制和时变风控，而不是单点价格预测。"
    if any(k in t for k in ["order book", "microstructure", "order flow"]):
        return "这篇内容围绕订单簿微结构与订单流展开，重点是解释盘口形态、撮合机制和成交冲击如何共同决定短期价格变化与执行成本。对量化研究而言，这类内容最值得转成盘口特征、冲击标签和分时执行约束。"
    if item.kind == "新闻":
        return "这条信息反映的是近 48 小时内市场结构、流动性或交易机制的变化。它的价值不只是新闻本身，而在于能否转成事件标签，并检验其对波动率、成交质量、跨资产相关性或风险偏好的影响。"
    return "这条内容与量化研究主线相关，核心价值在于提供一个可快速验证的新假设：要么帮助改进特征/标签/模型设计，要么帮助理解交易成本、流动性或市场结构变化。建议优先抓取原文中的数据口径、评价指标和实验设定，再决定是否纳入实验池。"


def chinese_brief_summary(item: Item) -> str:
    t = f"{item.title} {item.summary}".lower()
    if "slippage-at-risk" in t:
        return "提出前瞻性的滑点风险框架，可直接用于永续合约流动性风险监控。"
    if "algoxpert" in t or "overfitting" in t:
        return "给出量化策略反过拟合的流程化框架，适合改造研究验收规范。"
    if "dynamic fees" in t and "dex" in t:
        return "讨论 DEX 动态费率竞争如何影响订单流分配与执行滑点。"
    if "uncertainty quantification" in t or "selective prediction" in t:
        return "讨论不确定性边界与拒绝输出机制，适合迁移到信号上线闸门。"
    if "adaptive llm decoding" in t:
        return "强调按状态动态分配推理预算，对研究基础设施与在线调度有启发。"
    if any(k in t for k in ["order book", "microstructure", "order flow"]):
        return "围绕订单簿/订单流微结构，适合提炼盘口特征与执行约束。"
    if any(k in t for k in ["liquidity", "macro", "yield", "rate"]):
        return "反映流动性或宏观预期变化，适合转成事件驱动观察标签。"
    if item.kind == "新闻":
        return "反映近期市场结构或交易机制变化，适合做事件标签跟踪。"
    return "提供了新的研究线索，值得作为实验池候选。"


def chinese_line_summary(item: Item, line_kind: str) -> str:
    t = f"{item.title} {item.summary}".lower()
    title = item.title

    if line_kind == "research":
        if "slippage-at-risk" in t:
            return "该研究把滑点风险从历史回看改为盘口前瞻估计，可直接用于永续合约的实时风险预警与仓位约束设计。落地时建议联动清算事件和深度变化做分层回测。"
        if "overfitting" in t or "algoxpert" in t:
            return "该框架强调从参数稳定区间出发而非单点最优，并通过 IS/WFA/OOS 分段检验削弱过拟合。对研究团队价值在于把验收流程标准化，减少事后调参偏差。"
        if "dex" in t and "dynamic fees" in t:
            return "文章揭示 DEX 在动态费率下会围绕订单流展开策略博弈，进而改变执行滑点与噪音交易成本。可把费率机制变更视作事件变量，评估其对流动性迁移的影响。"
        if "order book" in t or "microstructure" in t:
            return "内容聚焦订单簿结构、订单流与执行冲击的联动关系。建议将盘口斜率、队列位置与撤单密度纳入特征池，并按交易时段分层检验稳定性。"
        if "reinforcement" in t or "imitation" in t:
            return "该方向强调在动态环境下联合优化收益、成本与库存风险，核心用途是执行策略与时变风控控制器。实践上建议先做离线策略评估，再做小规模在线实验。"
        if "survey" in t:
            return "这是一篇综述类工作，价值在于系统整理已有方法与评估口径。适合作为研究地图，用来快速定位可复现路线与尚未覆盖的实验空白。"
        if "benchmark" in t or "framework" in t:
            return "该条目偏研究基础设施，重点在于提供标准化评估流程或可比较基准。可直接用于统一实验口径，减少不同策略之间“不可比”的问题。"
        if item.kind == "GitHub":
            return f"该开源仓库（{title}）可直接用于搭建研究/回测原型，价值在于缩短从想法到实验的路径。建议先做样例复现，再按你的数据口径替换模块。"
        return f"该研究围绕《{title}》提出了可复现的技术路线。建议先抽取其核心变量定义、评价指标与实验设定，再做小样本复现验证可迁移性。"

    # markets
    if "native rollups" in t:
        return "这条更新指向以太坊扩容与验证机制可能出现结构变化，若落地将影响 L2 成本、吞吐与生态资金分布。短期可先作为“基础设施催化”事件持续跟踪。"
    if "funding rate" in t:
        return "资金费率转负通常意味着永续市场短期情绪偏空、套保需求上升。若与持仓增长同时出现，往往对应更高波动与更频繁的流动性抽离。"
    if "orderbook" in t or "order book" in t:
        return "订单簿失衡通常先体现在盘口深度不对称和冲击成本上升，再反馈到价格波动加速。实操上可结合挂单深度和主动成交占比来判断失衡持续性。"
    if "local high" in t or "liquidity sweep" in t:
        return "价格冲高后的流动性扫单信号常用于识别“突破延续”与“假突破回落”分叉。建议联动成交量、资金费率和基差变化做确认。"
    if any(k in t for k in ["exchange", "listing", "fee", "contract", "venue"]):
        return "这类交易所规则或费用变化通常会先重塑交易成本和做市意愿，再影响策略容量与执行质量。建议同步观察价差、深度和冲击成本三项指标。"
    if any(k in t for k in ["bitcoin", "ether", "crypto", "perpetual"]):
        return "该信息与加密市场结构直接相关，重点在于现货、永续与资金费率链路是否出现背离。可作为短周期波动与流动性切换的先行信号。"
    if any(k in t for k in ["liquidity", "yield", "macro", "rate"]):
        return "这条市场更新反映流动性或风险偏好正在变化，可能先影响成交深度与波动结构，再传导到跨资产相关性。建议将其标记为宏观/流动性事件并持续跟踪。"
    return "该市场信息可作为事件驱动观察点，建议结合成交活跃度、相关性与波动率变化做持续跟踪，而不是仅看单条新闻方向。"


def conclusion_detail(item: Item, idx: int) -> str:
    base = chinese_core_summary(item)
    if len(base) > 90:
        base = base[:90] + "…"
    return f"想法{idx}：围绕《{item.title}》的核心信息，先明确可检验假设与评价指标，再做分层回测验证稳健性。重点解读：{base}"


def filter_recent_items(items: list[Item], now: datetime, max_age_hours: int = 24) -> list[Item]:
    recent: list[Item] = []
    for item in items:
        try:
            published = datetime.fromisoformat(item.published_at)
        except Exception:
            recent.append(item)
            continue
        age = now - published
        if timedelta(hours=-6) <= age <= timedelta(hours=max_age_hours):
            recent.append(item)
    return recent


def build_readme_teaser(focus_items: list[Item]) -> str:
    text = " ".join(item.title for item in focus_items[:3]).lower()
    tags: list[str] = []
    if "slippage-at-risk" in text or "liquidity risk" in text:
        tags.append("SaR前瞻量化流动性风险")
    if "algoxpert" in text or "overfitting" in text:
        tags.append("反过拟合研究框架")
    if "dex" in text or "dynamic fees" in text:
        tags.append("DEX动态费率博弈")
    if "order book" in text or "microstructure" in text:
        tags.append("微结构信号新线索")
    if not tags and focus_items:
        title = focus_items[0].title
        short = title if len(title) <= 28 else title[:28] + "…"
        tags.append(short)
    return " + ".join(tags[:2]) + "：今天最值得盯的量化线索"


def update_readme(base: Path, dt_file: str, focus_items: list[Item]) -> Path:
    reports_dir = base / "reports" / "github"
    report_files = sorted(reports_dir.glob("*.md"), reverse=True)
    teaser = build_readme_teaser(focus_items)
    repo_base = "https://github.com/yinshuo-thu/Quant_Ideas_Everyday/blob/main/reports/github/"

    def report_url(path_name: str) -> str:
        return repo_base + urllib.parse.quote(path_name)

    lines = [
        "# Quant Ideas Everyday",
        "",
        "Daily Quant Ideas Digest repo mirror. New reports are published under `reports/github/`.",
        "",
        "## Latest Report",
        f"- [{dt_file}｜{teaser}]({report_url(dt_file + '.md')})",
        "",
        "## Recent Reports",
    ]
    seen = set()
    for path in report_files[:10]:
        stem = path.stem
        if stem in seen:
            continue
        seen.add(stem)
        if stem == dt_file:
            continue
        lines.append(f"- [{stem}｜Daily Quant Ideas Digest]({report_url(path.name)})")
    if len(lines) == 7:
        lines.append("- 暂无历史报告")
    lines.append("")
    readme_path = base / "README.md"
    readme_path.write_text("\n".join(lines), encoding="utf-8")
    return readme_path


def build_markdown(
    now: datetime,
    sources_covered: str,
    focus_items: list[Item],
    research_items: list[Item],
    market_items: list[Item],
    backup_items: list[Item],
    github_status: str,
    notion_status: str,
    feishu_status: str,
    failure_reason: str,
) -> str:
    lines: list[str] = []
    lines.append("Daily Quant Ideas Digest")
    lines.append("")
    lines.append("## Metadata")
    lines.append(f"- Generated At: {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append("- Focus: Quant Ideas + Markets News")
    lines.append(f"- Sources Covered: {sources_covered}")
    lines.append("")
    lines.append("## 一、今日最值得关注（最多 7 条）")

    if not focus_items:
        lines.append("- 今日暂无达到 4/5 的高质量条目，建议先看备选阅读并补充信息源。")

    for idx, item in enumerate(focus_items[:7], 1):
        ins = inspiration(item)
        core = chinese_core_summary(item)
        lines += [
            f"- 标题：{item.title}",
            f"- 类型：{item.kind}",
            f"- 来源：{item.source}",
            f"- 发布时间：{to_short_date(item.published_at)}",
            f"- 链接：{item.link}",
            f"- 评分：{item.score}/5",
            f"- 核心摘要：{core}",
            "- 实用启发：",
            f"  - 研究假设：{ins['hypothesis']}",
            f"  - 建模路径：{ins['model_path']}",
            f"  - 数据与监控：{ins['data_watch']}",
            f"  - 执行落地：{ins['execution']}",
            "",
        ]
        if idx < len(focus_items[:7]):
            lines += ["<!-- SPACER -->", ""]

    lines.append("## 二、Research Line")
    for topic in RESEARCH_TOPICS.keys():
        lines.append(f"### {topic}")
        bucket = [x for x in research_items if x.topic == topic][:6]
        if not bucket:
            lines.append("- 暂无高相关更新")
        else:
            for item in bucket:
                lines += [
                    f"- 标题：{item.title}",
                    f"  - 内容：{chinese_line_summary(item, 'research')}",
                    f"  - 链接：{item.link}",
                    "",
                ]
        lines.append("")

    lines.append("## 三、Markets Line")
    for topic in MARKET_TOPICS.keys():
        lines.append(f"### {topic}")
        bucket = [x for x in market_items if x.topic == topic][:6]
        if not bucket:
            lines.append("- 暂无高相关更新")
        else:
            for item in bucket:
                lines += [
                    f"- 标题：{item.title}",
                    f"  - 内容：{chinese_line_summary(item, 'markets')}",
                    f"  - 链接：{item.link}",
                    f"  - 潜在交易含义：{implication_for_market(item)}",
                    "",
                ]
        lines.append("")

    lines.append("## 四、备选阅读（评分 3/5）")
    if not backup_items:
        lines.append("- 暂无")
    else:
        for item in backup_items[:12]:
            lines.append(f"- {item.title}｜{chinese_brief_summary(item)}｜{item.link}")
    lines.append("")

    lines.append("## 五、今日结论")
    lines.append("**今日最重要的 3 个新想法**")
    top3 = focus_items[:3]
    if not top3:
        lines.append("- 暂无 4/5 以上信号，先补足信息源质量。")
    else:
        for idx, item in enumerate(top3, 1):
            lines.append(f"- {conclusion_detail(item, idx)}")
    lines += ["", "<!-- SPACER -->", ""]

    lines.append("**未来 7 天最值得验证的 3 个实验方向**")
    lines += [
        "- 方向1（微结构执行）：围绕盘口斜率、队列位置与撤单密度构建短周期信号，并按不同时段、波动分位、深度分位做分层回测，确认信号在高冲击环境下是否仍稳定。",
        "- 方向2（事件标签体系）：把交易所规则变化、费用调整、合约上新和流动性异常统一编码成事件标签，检验其对滑点、成交率、策略容量与风格暴露的持续影响。",
        "- 方向3（跨市场联动）：在加密市场把资金费率、未平仓量、基差与链上稳定币流入做联合建模，观察“风险偏好切换 → 流动性迁移 → 波动放大”这条链路是否可预测。",
        "",
        "<!-- SPACER -->",
        "",
    ]

    lines.append("**今日最值得持续跟踪的 3 个市场主题**")
    lines += [
        "- 主题1（流动性再定价）：跟踪全球流动性与利率预期变化对跨资产相关性的再定价过程，重点看权益-加密-商品之间的同步性是否增强或断裂。",
        "- 主题2（交易机制冲击）：跟踪交易所费用、撮合规则和产品结构变化，重点看其是否导致做市深度下降、价差抬升和冲击成本上行。",
        "- 主题3（加密结构变化）：持续监控现货-永续-期货三市场的联动强度，重点识别资金费率拐点与持仓结构变化是否先于价格波动扩散。",
    ]
    lines.append("")

    lines.append("## 六、运行与同步状态")
    lines.append(f"- GitHub：{github_status}")
    lines.append(f"- Notion：{notion_status}")
    lines.append(f"- 飞书：{feishu_status}")
    lines.append(f"- 失败原因与重试建议：{failure_reason}")
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def collect_items(base: Path) -> tuple[list[Item], list[str], list[str]]:
    feed_specs = [
        {"name": "arXiv q-fin", "url": "https://export.arxiv.org/rss/q-fin", "kind": "论文", "line": "research"},
        {"name": "arXiv stat.ML", "url": "https://export.arxiv.org/rss/stat.ML", "kind": "论文", "line": "research"},
        {"name": "arXiv cs.LG", "url": "https://export.arxiv.org/rss/cs.LG", "kind": "论文", "line": "research"},
        {"name": "WSJ Markets RSS", "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml", "kind": "新闻", "line": "markets"},
        {"name": "Investing.com RSS", "url": "https://www.investing.com/rss/news.rss", "kind": "新闻", "line": "markets"},
        {"name": "Cointelegraph Markets", "url": "https://cointelegraph.com/rss/tag/markets", "kind": "新闻", "line": "markets"},
        {"name": "Binance Announcements", "url": "https://www.binance.com/en/support/announcement/rss", "kind": "新闻", "line": "markets"},
        {"name": "The Block RSS", "url": "https://www.theblock.co/rss.xml", "kind": "新闻", "line": "markets"},
    ]

    items: list[Item] = []
    source_names: list[str] = []
    source_errors: list[str] = []

    for spec in feed_specs:
        try:
            xml_text = cache_fetch(base, spec["name"], spec["url"])
            parsed = parse_rss_items(xml_text, spec["name"], spec["kind"], spec["line"])
            if not parsed:
                source_errors.append(f"{spec['name']}: parsed 0 items")
                continue
            items.extend(parsed)
            source_names.append(spec["name"])
        except Exception as e:
            source_errors.append(f"{spec['name']}: {e}")

    try:
        gh_items = fetch_github_items()
        if gh_items:
            items.extend(gh_items)
            source_names.append("GitHub Search API")
        else:
            source_errors.append("GitHub Search API: returned 0 items")
    except Exception as e:
        source_errors.append(f"GitHub Search API: {e}")

    return items, source_names, source_errors


def run(base: Path, run_time: datetime, github_status: str, notion_status: str, feishu_status: str, failure_reason: str) -> dict[str, Any]:
    ensure_dirs(base)

    raw_items, source_names, source_errors = collect_items(base)
    raw_items = filter_recent_items(raw_items, run_time, max_age_hours=24)
    all_items = dedup(raw_items)

    for item in all_items:
        item.score = score_item(item, run_time)
        text = f"{item.title} {item.summary}"
        if item.line == "research":
            item.topic = pick_topic(text, RESEARCH_TOPICS, "Tools / Repos / Research Infrastructure")
        else:
            item.topic = pick_topic(text, MARKET_TOPICS, "Theme / Catalyst / Event-driven")

    all_items.sort(key=lambda x: (x.score, x.published_at), reverse=True)

    focus_items = [x for x in all_items if x.score >= 4][:7]
    backup_items = [x for x in all_items if x.score == 3][:12]

    research_items = [x for x in all_items if x.line == "research" and x.score >= 3]
    market_items = [x for x in all_items if x.line == "markets" and x.score >= 3]

    dt_file = run_time.strftime("%Y-%m-%d - %H%M")
    md_path = base / "reports" / "daily" / f"{dt_file}.md"
    github_md_path = base / "reports" / "github" / f"{dt_file}.md"
    json_path = base / "reports" / "json" / f"{dt_file}.json"

    sources_covered = "arXiv / RSS / Blogs / GitHub / WeChat Articles / Markets / Tools"
    markdown = build_markdown(
        now=run_time,
        sources_covered=sources_covered,
        focus_items=focus_items,
        research_items=research_items,
        market_items=market_items,
        backup_items=backup_items,
        github_status=github_status,
        notion_status=notion_status,
        feishu_status=feishu_status,
        failure_reason=failure_reason,
    )

    payload = {
        "generated_at": run_time.isoformat(),
        "timezone": "Asia/Singapore",
        "sources": source_names,
        "source_errors": source_errors,
        "counts": {
            "raw": len(raw_items),
            "deduped": len(all_items),
            "focus": len(focus_items),
            "backup": len(backup_items),
        },
        "focus_items": [asdict(x) for x in focus_items],
        "research_items": [asdict(x) for x in research_items],
        "market_items": [asdict(x) for x in market_items],
        "backup_items": [asdict(x) for x in backup_items],
        "sync_status": {
            "github": github_status,
            "notion": notion_status,
            "feishu": feishu_status,
            "failure_reason": failure_reason,
        },
    }

    md_path.write_text(markdown, encoding="utf-8")
    github_md_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Exports for downstream sync
    (base / "export" / "notion" / f"{dt_file}.md").write_text(markdown, encoding="utf-8")
    (base / "export" / "feishu" / f"{dt_file}.md").write_text(markdown, encoding="utf-8")
    readme_path = update_readme(base, dt_file, focus_items)

    state = {
        "last_run_at": run_time.isoformat(),
        "last_markdown": str(md_path),
        "last_json": str(json_path),
        "fingerprint": hashlib.sha256(markdown.encode("utf-8")).hexdigest(),
        "item_count": len(all_items),
    }
    (base / "state" / "last_run.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "markdown_path": str(md_path),
        "github_markdown_path": str(github_md_path),
        "readme_path": str(readme_path),
        "json_path": str(json_path),
        "focus_count": len(focus_items),
        "dedup_count": len(all_items),
        "raw_count": len(raw_items),
        "source_errors": source_errors,
        "dt_file": dt_file,
    }


def write_log(base: Path, dt_tag: str, content: str) -> Path:
    path = base / "logs" / f"run-{dt_tag}.log"
    path.write_text(content, encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default=".", help="project root")
    parser.add_argument("--github-status", default="待更新")
    parser.add_argument("--notion-status", default="待更新")
    parser.add_argument("--feishu-status", default="待更新")
    parser.add_argument("--failure-reason", default="待更新")
    args = parser.parse_args()

    base = Path(args.base).expanduser().resolve()
    run_time = now_sg()
    dt_tag = run_time.strftime("%Y%m%d-%H%M")

    result = run(
        base=base,
        run_time=run_time,
        github_status=args.github_status,
        notion_status=args.notion_status,
        feishu_status=args.feishu_status,
        failure_reason=args.failure_reason,
    )

    log_text = textwrap.dedent(
        f"""
        [quant_ideas_pipeline]
        generated_at={run_time.isoformat()}
        base={base}
        raw_count={result['raw_count']}
        dedup_count={result['dedup_count']}
        focus_count={result['focus_count']}
        source_error_count={len(result['source_errors'])}
        markdown_path={result['markdown_path']}
        github_markdown_path={result['github_markdown_path']}
        readme_path={result['readme_path']}
        json_path={result['json_path']}
        """
    ).strip() + "\n"
    log_path = write_log(base, dt_tag, log_text)

    print(json.dumps({"ok": True, "result": result, "log_path": str(log_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
