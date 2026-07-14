# -*- coding: utf-8 -*-
"""
每日财经新闻与股票推荐定时任务
================================
- 每天 08:00 自动获取最新财经新闻
- 基于新闻关键词做情绪分析，推荐对应板块龙头股
- 结果输出到控制台 + output/report_YYYYMMDD.json

重要声明
--------
金融市场不存在"一定会涨"的股票。本程序仅基于公开新闻做关键词
情绪匹配，输出仅供参考，不构成任何投资建议。请理性投资、自负盈亏。
"""

import os
import sys
import json
import time
import hmac
import base64
import hashlib
import logging
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests

# 定时器（仅本地常驻模式需要，CI 模式不必）
try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError:
    BlockingScheduler = None

try:
    import akshare as ak
except ImportError:
    print("缺少依赖，请先执行: pip install -r requirements.txt")
    sys.exit(1)


# ==================== 配置区 ====================
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

RUN_HOUR = 8        # 每天执行的小时
RUN_MINUTE = 0      # 分钟
NEWS_LIMIT = 20     # 每个数据源抓取条数

# 板块 -> 关键词 + 代表性股票
# 注：以下股票为对应板块的代表性标的，不构成投资建议
SECTOR_MAP = {
    "人工智能": {
        "keywords": ["人工智能", "AI", "大模型", "ChatGPT", "算力", "芯片", "GPU", "智能"],
        "stocks": [
            {"code": "002230", "name": "科大讯飞", "reason": "国内语音AI龙头，大模型赛道核心标的"},
            {"code": "688256", "name": "寒武纪",   "reason": "AI芯片龙头，受益于算力需求增长"},
        ],
    },
    "新能源": {
        "keywords": ["新能源", "光伏", "锂电", "储能", "风电", "电动车", "充电桩"],
        "stocks": [
            {"code": "300750", "name": "宁德时代", "reason": "全球动力电池龙头"},
            {"code": "601012", "name": "隆基绿能", "reason": "光伏硅片与组件龙头"},
        ],
    },
    "半导体": {
        "keywords": ["半导体", "芯片", "集成电路", "晶圆", "封装", "光刻", "国产替代"],
        "stocks": [
            {"code": "688981", "name": "中芯国际", "reason": "国内晶圆代工龙头"},
            {"code": "002049", "name": "紫光国微", "reason": "国内特种集成电路龙头"},
        ],
    },
    "医药生物": {
        "keywords": ["医药", "生物", "疫苗", "创新药", "医疗器械", "CRO", "中药"],
        "stocks": [
            {"code": "600276", "name": "恒瑞医药", "reason": "国内创新药龙头"},
            {"code": "300015", "name": "爱尔眼科", "reason": "民营眼科医疗龙头"},
        ],
    },
    "消费": {
        "keywords": ["消费", "白酒", "食品", "零售", "免税", "旅游", "家电"],
        "stocks": [
            {"code": "600519", "name": "贵州茅台", "reason": "高端白酒龙头"},
            {"code": "000858", "name": "五粮液",   "reason": "白酒龙头之一"},
        ],
    },
    "金融": {
        "keywords": ["券商", "银行", "保险", "金融", "降准", "降息"],
        "stocks": [
            {"code": "601318", "name": "中国平安", "reason": "综合金融龙头"},
            {"code": "600030", "name": "中信证券", "reason": "券商龙头"},
        ],
    },
    "军工": {
        "keywords": ["军工", "国防", "航天", "航空", "装备", "导弹"],
        "stocks": [
            {"code": "600760", "name": "中航沈飞", "reason": "歼击机总装龙头"},
            {"code": "600893", "name": "航发动力", "reason": "航空发动机龙头"},
        ],
    },
}

# 利好/利空关键词（用于情绪打分）
POSITIVE_KEYWORDS = [
    "突破", "增长", "大涨", "涨停", "订单", "中标", "获批", "上市",
    "利好", "提振", "加速", "创新高", "超预期", "爆发", "上调", "增持",
    "回购", "减税", "补贴", "扶持", "支持", "推进", "落地", "签约",
]
NEGATIVE_KEYWORDS = [
    "下跌", "暴跌", "亏损", "减持", "处罚", "问询", "退市", "爆雷",
    "下调", "不及预期", "警告", "事故", "召回", "诉讼",
]


# ==================== 日志 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "advisor.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("advisor")


# ==================== 钉钉推送 ====================
def send_dingtalk(text):
    """通过钉钉自定义机器人推送消息。
    读取环境变量:
      DINGTALK_WEBHOOK: 机器人 webhook 地址 (必填，否则跳过推送)
      DINGTALK_SECRET:  加签密钥 (可选；若机器人安全设置选了"加签"则必填)

    提醒: 若机器人安全设置选了"自定义关键词"，请把关键词设为"财经推荐"
          (本程序发出的消息均以该词开头)。
    """
    webhook = os.environ.get("DINGTALK_WEBHOOK", "").strip()
    if not webhook:
        log.info("未配置 DINGTALK_WEBHOOK，跳过钉钉推送")
        return False

    # 加签
    secret = os.environ.get("DINGTALK_SECRET", "").strip()
    if secret:
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(
            secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        sep = "&" if "?" in webhook else "?"
        webhook = f"{webhook}{sep}timestamp={timestamp}&sign={sign}"

    headers = {"Content-Type": "application/json"}
    payload = {"msgtype": "text", "text": {"content": text[:20000]}}  # 钉钉单条上限 20000 字符
    try:
        r = requests.post(webhook, headers=headers, json=payload, timeout=15)
        r.raise_for_status()
        resp = r.json()
        if resp.get("errcode", -1) == 0:
            log.info("钉钉推送成功")
            return True
        log.error(f"钉钉推送失败: {resp}")
        return False
    except Exception as e:
        log.error(f"钉钉推送异常: {e}")
        return False


# ==================== 新闻获取 ====================
def fetch_news():
    """从 akshare 多个免费源抓取财经新闻，任一失败不影响整体"""
    news_list = []

    # 1) 央视新闻联播文字稿（数据稳定）
    try:
        df = ak.news_cctv(date=datetime.now().strftime("%Y%m%d"))
        if df is not None and len(df) > 0:
            for _, row in df.head(NEWS_LIMIT).iterrows():
                news_list.append({
                    "title":   str(row.get("title", "")),
                    "content": str(row.get("content", "")),
                    "source":  "央视新闻",
                })
    except Exception as e:
        log.warning(f"央视新闻获取失败: {e}")

    # 2) 东方财富全球财经快讯
    try:
        df = ak.stock_info_global_em()
        if df is not None and len(df) > 0:
            for _, row in df.head(NEWS_LIMIT).iterrows():
                # 字段名可能是中文或英文，兼容处理
                title   = row.get("标题")   or row.get("title")   or ""
                content = row.get("内容")   or row.get("content") or ""
                if title:
                    news_list.append({
                        "title":   str(title),
                        "content": str(content),
                        "source":  "东财快讯",
                    })
    except Exception as e:
        log.warning(f"东财快讯获取失败: {e}")

    return news_list


# ==================== 分析推荐 ====================
def analyze_and_recommend(news_list):
    """对新闻做关键词情绪分析，给出板块推荐与代表股票"""
    scores = {}        # sector -> 情绪分
    matched_news = {}  # sector -> [news_title]

    for news in news_list:
        text = (news["title"] + " " + news["content"]).lower()
        for sector, cfg in SECTOR_MAP.items():
            if not any(kw.lower() in text for kw in cfg["keywords"]):
                continue
            pos = sum(1 for kw in POSITIVE_KEYWORDS if kw in text)
            neg = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text)
            scores[sector] = scores.get(sector, 0) + (pos - neg)
            matched_news.setdefault(sector, []).append(news["title"])

    # 至少要有 1 分利好才推荐
    if not scores or max(scores.values()) < 1:
        return None, "今日新闻无明显板块利好，不建议追高，建议观望。"

    best_sector = max(scores, key=scores.get)
    best_score  = scores[best_sector]
    stock       = SECTOR_MAP[best_sector]["stocks"][0]
    matched     = matched_news.get(best_sector, [])

    reason = (
        f"今日「{best_sector}」板块新闻情绪偏利好（情绪分 {best_score}），"
        f"匹配到的新闻包括: {'; '.join(matched[:3])}。"
        f"推荐关注 {stock['name']}({stock['code']})，{stock['reason']}。"
    )

    return {
        "sector": best_sector,
        "score":  best_score,
        "stock":  stock,
        "matched_news": matched,
    }, reason


# ==================== 单次执行 ====================
def run_once():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"==== 开始执行 {now} ====")

    news = fetch_news()
    log.info(f"共获取新闻 {len(news)} 条")

    if not news:
        log.warning("未获取到任何新闻，跳过本次推荐")
        return

    rec, reason = analyze_and_recommend(news)
    if rec:
        log.info(f"【推荐板块】{rec['sector']}（情绪分 {rec['score']}）")
        log.info(f"【推荐股票】{rec['stock']['name']}({rec['stock']['code']})")
        log.info(f"【推荐理由】{reason}")
    else:
        log.info(f"【今日提示】{reason}")

    log.info("【风险提示】股票市场有风险，'一定会涨'不存在，本推荐仅供参考，请理性投资。")

    result = {
        "time": now,
        "news_count": len(news),
        "news": [{"title": n["title"], "source": n["source"]} for n in news[:10]],
        "recommendation": rec,
        "reason": reason,
    }
    out_file = OUTPUT_DIR / f"report_{datetime.now().strftime('%Y%m%d')}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    log.info(f"结果已保存: {out_file}")

    # 钉钉推送 (消息以"财经推荐"开头，便于通过钉钉关键词安全校验)
    push_text = f"【财经推荐 {now}】\n"
    if rec:
        push_text += (
            f"推荐板块: {rec['sector']}（情绪分 {rec['score']}）\n"
            f"推荐股票: {rec['stock']['name']}({rec['stock']['code']})\n"
            f"推荐理由: {reason}\n"
        )
    else:
        push_text += f"今日提示: {reason}\n"

    # 附上今日新闻摘要 (最多 10 条，避免超钉钉单条 20000 字符上限)
    if news:
        push_text += "\n--- 今日新闻摘要 ---\n"
        for i, n in enumerate(news[:10], 1):
            push_text += f"{i}. {n['title']} [{n['source']}]\n"

    push_text += "\n风险提示: 股市有风险，'一定会涨'不存在，本推荐仅供参考，请理性投资。"
    send_dingtalk(push_text)


# ==================== 入口 ====================
def main():
    # 立即执行一次，方便测试 / CI 触发：python finance_stock_advisor.py --now
    if "--now" in sys.argv:
        run_once()
        return

    if BlockingScheduler is None:
        log.error("未安装 apscheduler，无法启动定时模式。可用 --now 立即执行一次。")
        run_once()
        return

    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        run_once,
        CronTrigger(hour=RUN_HOUR, minute=RUN_MINUTE),
        id="daily_advisor",
        misfire_grace_time=3600,
    )
    log.info(f"定时任务已启动：每天 {RUN_HOUR:02d}:{RUN_MINUTE:02d} 执行")
    log.info("立即测试可运行: python finance_stock_advisor.py --now")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("任务已停止")


if __name__ == "__main__":
    main()
