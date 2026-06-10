# dags/telegram/telegram_watchlist_news_classifier.py

"""
Watchlist News Classifier DAG

Fetches the 5 latest news articles for each symbol in the watchlist via
Yahoo Finance RSS (free, no API key), extracts up to 2000 chars of article
text, and uses Ollama (qwen3.5:4b) to rate each article 1-10 for market
impact/interest.

Sends a Telegram message per symbol only when at least one article scores
>= SCORE_THRESHOLD. Articles are sorted highest score first.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

SCORE_THRESHOLD = 7   # only alert on articles at or above this score
NEWS_LIMIT      = 5   # articles to fetch per symbol
OLLAMA_MODEL    = "qwen3.5:4b"
OLLAMA_URL      = "http://192.168.1.102:11434/api/generate"


# ============================================================
# TASK FUNCTIONS
# ============================================================

def fetch_and_classify_news():
    import json
    import re
    import time
    import requests
    from bs4 import BeautifulSoup
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.models import Variable

    TELEGRAM_BOT_TOKEN = Variable.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID   = Variable.get("TELEGRAM_CHAT_ID")

    # ── Helpers ────────────────────────────────────────────────────────────

    def send_telegram(message):
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            resp = requests.post(url, data=payload, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Telegram error: {resp.text}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")

    def fetch_article_text(url, fallback=""):
        """Fetch article URL and return first 2000 chars of visible text."""
        try:
            resp = requests.get(
                url,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0 (compatible; news-classifier/1.0)"},
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
            return text[:2000] if text.strip() else fallback[:2000]
        except Exception as e:
            logger.warning(f"Could not fetch article at {url}: {e} — using FMP snippet")
            return fallback[:2000]

    def classify_article(symbol, title, text):
        """Call Ollama to rate article impact 1-10. Returns (score, reason) or (None, None)."""
        prompt = (
            f"/no_think\n"
            f"You are a financial news analyst. Evaluate this news article about {symbol}.\n\n"
            f"Rate its market impact/interest from 1 to 10:\n"
            f"1-3 = Low (routine, non-material)\n"
            f"4-6 = Moderate (noteworthy but not urgent)\n"
            f"7-9 = High (significant, potential market-moving)\n"
            f"10  = Critical (earnings shock, M&A, regulatory action, major incident)\n\n"
            f"Title: {title}\n\n"
            f"Article excerpt:\n{text}\n\n"
            f'Respond ONLY with valid JSON and nothing else: {{"score": <integer 1-10>, "reason": "<one concise sentence>"}}'
        )
        try:
            resp = requests.post(
                OLLAMA_URL,
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "think": False},
                timeout=300,
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "")
            # Strip <think>...</think> blocks from reasoning models
            raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
            match = re.search(r"\{.*?\}", raw, re.DOTALL)
            if not match:
                logger.warning(f"No JSON found in Ollama response for {symbol}: {raw[:200]}")
                return None, None
            data = json.loads(match.group())
            score  = max(1, min(10, int(data["score"])))
            reason = str(data.get("reason", "")).strip()
            return score, reason
        except Exception as e:
            logger.error(f"Ollama classification failed for {symbol} — {title[:60]}: {e}")
            return None, None

    def score_emoji(score):
        if score >= 8:
            return "🔴"
        if score >= 6:
            return "🟡"
        return "🟢"

    # ── 1. Load watchlist symbols ──────────────────────────────────────────
    hook = PostgresHook(postgres_conn_id="postgres_insights_db")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM public.watchlist ORDER BY symbol")
            symbols = [row[0] for row in cur.fetchall()]

    if not symbols:
        logger.info("Watchlist is empty — nothing to process.")
        return

    logger.info(f"Processing {len(symbols)} symbols: {symbols}")

    # ── 2. Fetch, classify, and alert per symbol ───────────────────────────
    total_alerts = 0

    for symbol in symbols:
        logger.info(f"=== {symbol} ===")

        # Fetch latest news from Yahoo Finance RSS (free, no API key)
        try:
            import xml.etree.ElementTree as ET
            rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
            rss_resp = requests.get(
                rss_url,
                timeout=15,
                headers={"User-Agent": "Mozilla/5.0 (compatible; news-classifier/1.0)"},
            )
            rss_resp.raise_for_status()
            root = ET.fromstring(rss_resp.text)
            items = root.findall(".//item")[:NEWS_LIMIT]
            articles = [
                {
                    "title":         (item.findtext("title") or "").strip(),
                    "url":           (item.findtext("link")  or "").strip(),
                    "text":          (item.findtext("description") or "").strip(),
                    "publishedDate": (item.findtext("pubDate") or "")[:16],
                    "site":          "Yahoo Finance",
                }
                for item in items
                if item.findtext("title")
            ]
        except Exception as e:
            logger.error(f"{symbol}: Yahoo Finance RSS fetch failed — {e}")
            continue

        if not articles:
            logger.info(f"{symbol}: no news articles found")
            continue

        logger.info(f"{symbol}: fetched {len(articles)} articles")

        # Classify each article
        classified = []
        for article in articles:
            title     = article.get("title", "")
            url       = article.get("url", "")
            snippet   = article.get("text", "")
            published = article.get("publishedDate", "")[:10]
            site      = article.get("site", "")

            text = fetch_article_text(url, fallback=snippet)
            score, reason = classify_article(symbol, title, text)

            if score is None:
                continue

            classified.append({
                "title":     title,
                "url":       url,
                "published": published,
                "site":      site,
                "score":     score,
                "reason":    reason,
            })
            logger.info(f"  [{score}/10] {title[:70]}")
            time.sleep(0.3)   # small pause between Ollama calls

        # Filter and sort by score descending
        notable = [a for a in classified if a["score"] >= SCORE_THRESHOLD]
        notable.sort(key=lambda x: x["score"], reverse=True)

        if not notable:
            logger.info(f"{symbol}: no articles reached threshold ({SCORE_THRESHOLD}+) — skipping")
            continue

        # Build Telegram message
        lines = [f"📰 <b>News Alert — {symbol}</b>  ({len(notable)} of {len(classified)} articles scored {SCORE_THRESHOLD}+)\n"]
        for a in notable:
            lines.append(
                f"{score_emoji(a['score'])} <b>[{a['score']}/10]</b> <a href=\"{a['url']}\">{a['title']}</a>\n"
                f"  {a['reason']}\n"
                f"  <i>{a['site']} · {a['published']}</i>\n"
            )

        message = "\n".join(lines)
        send_telegram(message)
        total_alerts += 1
        logger.info(f"{symbol}: alert sent ({len(notable)} notable articles)")
        time.sleep(0.5)

    logger.info(f"Done — sent alerts for {total_alerts}/{len(symbols)} symbols.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="telegram_watchlist_news_classifier",
    description="Fetches latest news for watchlist symbols, classifies 1-10 with Ollama, sends Telegram alerts",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["watchlist", "news", "telegram", "ai"],
) as dag:

    task_fetch_and_classify = PythonOperator(
        task_id="fetch_and_classify_news",
        python_callable=fetch_and_classify_news,
        execution_timeout=timedelta(minutes=30),
    )
