"""
Intel Media Pulse — FastAPI backend
====================================
Monitors Israeli and global media for coverage of Intel / אינטל.

Endpoints:
  GET  /                       → Serves the dashboard HTML
  GET  /api/articles           → Filtered article list
  POST /api/refresh            → Manual trigger for article collection
  GET  /api/sources            → List of known sources
  GET  /api/stats              → Aggregated stats (counts + trend)

Storage: JSON files (articles.json, article_cache.json, sources.json)
Scheduling: APScheduler background jobs
  • Daily   → collect_articles()
  • Weekly  → discover_sources()
"""

import asyncio
import concurrent.futures
import base64
import hashlib
from collections import Counter

# No proxy required — WinHTTP is configured for direct internet access
_INTEL_PROXY = None
import socket
import json
import logging
import os
import time
import urllib.parse
import urllib.request
import re
import html

# Force IPv4 globally — the corporate network has broken/blackholed IPv6 to
# Google + most external publishers. Python's default `socket.getaddrinfo`
# returns IPv6 records first and blocks for the full TCP timeout before
# falling back, which causes RSS/feedparser/httpx requests to hang forever.
# PowerShell/.NET happen to prefer IPv4, which is why the user sees the same
# URLs working in `Invoke-WebRequest` while the refresh fetches all time out.
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    results = _orig_getaddrinfo(host, port, family, type, proto, flags)
    ipv4 = [r for r in results if r[0] == socket.AF_INET]
    return ipv4 or results


socket.getaddrinfo = _ipv4_only_getaddrinfo

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

# Thread pool for blocking I/O (feedparser, lxml)
_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=6)

# Cap all socket I/O (feedparser, urllib) at 12 s — prevents thread-pool
# hangs when Google News throttles requests.
socket.setdefaulttimeout(12)

import feedparser
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

# Google News URL resolver (for resolving redirect URLs to real article URLs)
from gnews_url_resolver import GoogleNewsURLResolver

# ---------------------------------------------------------------------------
# Config & paths
# ---------------------------------------------------------------------------
load_dotenv()

BASE_DIR = Path(__file__).parent
SOURCES_FILE = BASE_DIR / "sources.json"
ARTICLES_FILE = BASE_DIR / "articles.json"
CACHE_FILE = BASE_DIR / "article_cache.json"  # enrichment cache keyed by URL
QUALITY_REPORT_FILE = BASE_DIR / "quality_report.json"

TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "").strip()
# Hard retention window for persisted articles.
CACHE_MAX_DAYS = int(os.getenv("CACHE_MAX_DAYS", "30"))
# Max number of pending-body-check articles to resolve per collection run.
# Keep low: each fetch can take up to 8s; 50 × ceil(50/6) ≈ ~70s max.
BODY_CHECK_LIMIT = int(os.getenv("BODY_CHECK_LIMIT", "50"))
# Max number of articles to enrich per run (image/description).
ENRICH_LIMIT = int(os.getenv("ENRICH_LIMIT", "220"))
# Max body text length (chars) scanned for Intel keywords.
BODY_CHECK_CHARS = 120_000

# Intel keywords for relevance filtering
INTEL_KEYWORDS_HE = ["אינטל", "Intel"]
INTEL_KEYWORDS_EN = ["Intel", "אינטל"]
HEBREW_CHAR_RE = re.compile(r"[\u0590-\u05FF]")
# Match Intel tokens as standalone terms (avoid substring false-positives,
# e.g. "אינטל" inside "אינטליגנציה"). Allow standard Hebrew clitic prefixes
# (ב/ל/מ/ה/ו/ש/כ + their combinations like וב, כש, מהאינטל…) — these are
# legitimate Hebrew references to Intel ("באינטל"=at Intel, "מאינטל"=from
# Intel, "לאינטל"=to Intel, "שאינטל"=that Intel, "ובאינטל"=and at Intel).
# The trailing lookahead still blocks Hebrew suffixes that would otherwise
# allow false-positives (e.g. "אינטליגנציה").
INTEL_TOKEN_RE = re.compile(
    r"(?:"
    r"(?<![\u0590-\u05FFA-Za-z0-9_])(?:[\u05D1\u05DC\u05DE\u05D4\u05D5\u05E9\u05DB]{1,3})?אינטל(?![\u0590-\u05FFA-Za-z0-9_])"
    r"|(?<![A-Za-z0-9_])intel(?![A-Za-z0-9_])"
    r")",
    re.IGNORECASE,
)

# Body-check strictness: to accept an article whose title/description don't
# mention Intel, the body must (a) contain at least BODY_INTEL_MIN_HITS
# mentions and (b) have at least one mention within the first
# BODY_INTEL_LEAD_CHARS characters (the lead). Prevents accepting articles
# where Intel is only a passing reference deep in the text.
BODY_INTEL_MIN_HITS = int(os.getenv("BODY_INTEL_MIN_HITS", "5"))
BODY_INTEL_LEAD_CHARS = int(os.getenv("BODY_INTEL_LEAD_CHARS", "500"))
DATE_VERIFY_MAX = int(os.getenv("DATE_VERIFY_MAX", "400"))
DATE_VERIFY_RECHECK_HOURS = int(os.getenv("DATE_VERIFY_RECHECK_HOURS", "24"))

# Calendar-day filtering should follow Israel local time.
IL_TZ = ZoneInfo("Asia/Jerusalem")

# Google News RSS base
GNEWS_RSS_BASE = "https://news.google.com/rss/search?hl=iw&gl=IL&ceid=IL:iw&q={query}&num=100"

# Extra site-scoped domains we always query via Google News to improve
# discoverability for finance outlets that may not expose stable RSS feeds.
SITE_SCOPED_EXTRA_DOMAINS = [
    "bizportal.co.il",
    "il.investing.com",
]

# Manual URL include overrides for high-priority items that may be missed by
# source discovery. Can be extended via MONITOR_INCLUDE_URLS env var
# (comma-separated URLs).
MANUAL_INCLUDE_URLS = {
    "https://tech.walla.co.il/item/3832226",
    "https://www.bizportal.co.il/globalmarkets/news/article/20030477",
}
_extra_include_urls = {
    u.strip() for u in os.getenv("MONITOR_INCLUDE_URLS", "").split(",") if u.strip()
}
MANUAL_INCLUDE_URLS.update(_extra_include_urls)

# HTML fallback endpoints for sources that are intermittently blocked on RSS
# or rely heavily on Google News reachability.
HTML_FALLBACK_FEEDS: dict[str, list[str]] = {
    "globes": ["https://www.globes.co.il/news/home.aspx?fid=607"],
    "bizportal": ["https://www.bizportal.co.il/"],
    # calcalist.co.il blocks RSS/homepage in this environment (403), but
    # calcalistech.com / ctech.com remain reachable.
    "calcalist": ["https://www.calcalistech.com/", "https://www.ctech.com/"],
}

# Instantiate Google News URL resolver (converts redirect URLs to real article URLs)
_GNEWS_RESOLVER = GoogleNewsURLResolver(
    cache_file=str(BASE_DIR / "gnews_url_cache.json"),
    cache_ttl_hours=7 * 24  # Cache for 7 days
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("intel-media-pulse")

# ---------------------------------------------------------------------------
# App init
# ---------------------------------------------------------------------------
app = FastAPI(title="Intel Media Pulse", version="1.0.0")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# In-memory refresh status
_refresh_status = {
    "running": False,
    "last_run": None,
    "last_error": None,
    "initial_done": False,
    "coverage": {},
    "source_monitoring": {},
}
scheduler = AsyncIOScheduler(timezone="Asia/Jerusalem")

# ---------------------------------------------------------------------------
# JSON file helpers
# ---------------------------------------------------------------------------

def _read_json(path: Path, default):
    """Read JSON from disk; return default on error."""
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Could not read %s: %s", path, exc)
    return default


def _write_json(path: Path, data) -> None:
    """Atomically write JSON to disk."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)


def _load_sources() -> list[dict]:
    data = _read_json(SOURCES_FILE, {"sources": []})
    return data.get("sources", [])


def _load_articles() -> list[dict]:
    return _read_json(ARTICLES_FILE, [])


def _load_cache() -> dict:
    return _read_json(CACHE_FILE, {})


def _save_cache(cache: dict) -> None:
    _write_json(CACHE_FILE, cache)


def _url_id(url: str) -> str:
    """Stable short ID for a URL."""
    return hashlib.sha1(url.encode()).hexdigest()[:16]


def _normalize_url_for_dedupe(url: str) -> str:
    """Normalize URL for cross-source dedupe (rss/html/gnews)."""
    u = html.unescape((url or "").strip())
    if not u:
        return ""
    try:
        p = urllib.parse.urlparse(u)
        if not p.scheme or not p.netloc:
            return u
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = p.path or "/"
        if path != "/":
            path = path.rstrip("/")

        q = urllib.parse.parse_qsl(p.query or "", keep_blank_values=True)
        filtered_q = []
        for k, v in q:
            lk = (k or "").lower()
            if lk.startswith("utm_"):
                continue
            if lk in {"fbclid", "gclid", "mc_cid", "mc_eid", "oc"}:
                continue
            filtered_q.append((k, v))
        filtered_q.sort()
        query = urllib.parse.urlencode(filtered_q, doseq=True)
        return urllib.parse.urlunparse((p.scheme.lower(), host, path, "", query, ""))
    except Exception:
        return u


def _canonical_article_url(article: dict) -> str:
    """Return best canonical URL for dedupe across feed sources."""
    raw_url = (article.get("url") or "").strip()
    resolved = (article.get("resolved_url") or "").strip()

    if resolved and "news.google.com" not in resolved.lower():
        return _normalize_url_for_dedupe(resolved)

    if "news.google.com" in raw_url.lower():
        decoded = _decode_gnews_url(raw_url)
        if decoded and decoded != raw_url and "news.google.com" not in decoded.lower():
            return _normalize_url_for_dedupe(decoded)

    return _normalize_url_for_dedupe(raw_url)


def _extract_url_from_gnews_summary(summary_html: str) -> str:
    """Extract real article URL from Google News RSS summary HTML.

    GNews <description> looks like:
      <ol><li><a href="https://real-article-url.com/...">Title</a> ...</li></ol>

    Returns empty string if not found or if the URL is still a news.google.com link.
    """
    if not summary_html:
        return ""
    # Fast path: regex before any HTML parsing
    m = re.search(r'href=["\']?(https?://[^"\'>\s]+)', summary_html)
    if m:
        href = m.group(1)
        if "news.google.com" not in href and "." in href:
            return href
    return ""


def _extract_image_from_html(summary_html: str) -> str:
    """Extract the first likely image URL from an HTML snippet."""
    if not summary_html:
        return ""
    m = re.search(r'<img[^>]+src=["\']([^"\'>\s]+)', summary_html, flags=re.IGNORECASE)
    if not m:
        return ""
    src = html.unescape(m.group(1)).strip()
    if src.startswith("http") and "." in src:
        return src
    return ""


def _parse_feed_with_timeout(feed_url: str, timeout: int = 8):
    """Fetch RSS/Atom XML with a hard timeout, then parse from bytes."""
    try:
        req = urllib.request.Request(
            feed_url,
            headers={"User-Agent": "IntelMediaPulse/1.0 (+bot)"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read()
        return feedparser.parse(payload)
    except Exception as exc:
        log.warning("Feed fetch failed (%s): %s", feed_url, exc)
        return feedparser.FeedParserDict({"entries": []})


def _collect_from_html_fallback_sync(sources: list[dict]) -> list[dict]:
    """Collect Intel headlines from selected source homepages as fallback."""
    out: list[dict] = []
    seen_urls: set[str] = set()

    for src in sources:
        if not src.get("active", True):
            continue
        sid = (src.get("id") or "").strip()
        pages = HTML_FALLBACK_FEEDS.get(sid)
        if not pages:
            continue

        src_domain = (src.get("domain") or "").replace("www.", "").lower()
        allowed_domains = set((src.get("domains") or []) + [src_domain])
        # Calcalist fallback is served via ctech/calcalistech hosts.
        if sid == "calcalist":
            allowed_domains.update({"ctech.com", "www.ctech.com", "calcalistech.com", "www.calcalistech.com"})

        for page_url in pages:
            html_text = ""
            last_exc = None
            for _ in range(1):
                try:
                    req = urllib.request.Request(
                        page_url,
                        headers={"User-Agent": "Mozilla/5.0 (IntelMediaPulse/1.0)"},
                    )
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        html_text = resp.read().decode("utf-8", errors="ignore")
                    break
                except Exception as exc:
                    last_exc = exc
            if not html_text:
                log.warning("HTML fallback fetch failed (%s): %s", page_url, last_exc)
                continue

            soup = BeautifulSoup(html_text, "lxml")
            added_for_page = 0
            for a in soup.find_all("a", href=True):
                if added_for_page >= 12:
                    break
                href = (a.get("href") or "").strip()
                title = _clean_text(a.get_text(" ", strip=True))
                if not href or not title or len(title) < 18:
                    continue

                abs_url = urllib.parse.urljoin(page_url, href)
                parsed = urllib.parse.urlparse(abs_url)
                host = parsed.netloc.replace("www.", "").lower()
                if host and host not in {d.replace("www.", "").lower() for d in allowed_domains if d}:
                    continue

                if abs_url in seen_urls:
                    continue
                if not _is_intel_relevant(title):
                    continue
                if not _is_hebrew_article(title, "") and sid != "calcalist":
                    continue

                seen_urls.add(abs_url)
                added_for_page += 1
                out.append({
                    "id": _url_id(abs_url),
                    "url": abs_url,
                    "title": title,
                    "description": "",
                    "source": src.get("display_name", src.get("name", sid)),
                    "source_id": sid,
                    "published_at": datetime.now(timezone.utc).isoformat(),
                    "image": "",
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "via": "html_fallback",
                })

    return out


def _decode_gnews_url(url: str) -> str:
    """Attempt to decode a Google News RSS redirect URL to the real article URL.

    NOTE: Google changed their encoding format. Static base64 decode is no longer
    reliable. Use _extract_url_from_gnews_summary() at parse-time when possible,
    which reads the real URL from the feedparser summary HTML.

    Returns the original url unchanged if decoding fails.
    """
    if "news.google.com" not in url:
        return url
    m = re.search(r"/articles/([A-Za-z0-9_\-]+)", url)
    if not m:
        return url
    encoded = m.group(1)
    padding = (4 - len(encoded) % 4) % 4
    encoded += "=" * padding
    try:
        data = base64.urlsafe_b64decode(encoded)
        # Scan for embedded http(s):// in the binary payload
        for i in range(len(data) - 8):
            if data[i : i + 8] == b"https://" or data[i : i + 7] == b"http://":
                end = i
                while end < len(data) and 0x20 <= data[end] <= 0x7E:
                    end += 1
                candidate = data[i:end].decode("ascii", errors="ignore")
                if "." in candidate and len(candidate) >= 15:
                    return candidate
    except Exception:
        pass
    return url


# ---------------------------------------------------------------------------
# Relevance check
# ---------------------------------------------------------------------------

def _clean_text(value: str) -> str:
    """Normalize HTML-ish feed text into clean plain text."""
    if not value:
        return ""
    decoded = html.unescape(str(value))
    text = BeautifulSoup(decoded, "lxml").get_text(" ", strip=True)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _sanitize_article(article: dict) -> dict:
    """Return a normalized article object with clean text fields."""
    sanitized = dict(article)
    sanitized["title"] = _clean_text(article.get("title", ""))
    sanitized["description"] = _clean_text(article.get("description", ""))
    sanitized["source"] = _clean_text(article.get("source", ""))
    return sanitized


def _contains_hebrew(text: str) -> bool:
    """Return True when the text contains Hebrew characters."""
    if not text:
        return False
    return bool(HEBREW_CHAR_RE.search(text))


def _is_hebrew_article(title: str, description: str = "") -> bool:
    """Accept only article metadata that is clearly Hebrew."""
    return _contains_hebrew(_clean_text(title)) or _contains_hebrew(_clean_text(description))


def _is_intel_relevant(text: str) -> bool:
    """True if text mentions Intel/אינטל as a standalone token."""
    cleaned = _clean_text(text)
    if not cleaned:
        return False
    return bool(INTEL_TOKEN_RE.search(cleaned))


# Pattern: ticker-style title with 3+ "company X%" pairs (e.g. "מארוול 7%, אינטל
# 1.8%, נבידיה 2%"). In such titles Intel is one of many listings, not the
# subject. Require >= 3 percent markers to avoid flagging normal financial
# headlines that happen to mention one percentage.
_TICKER_LIST_RE = re.compile(r"\d+(?:\.\d+)?\s*%")
# Instrument pages syndicated as headlines (e.g., "Intel חדשות מניית (INTC)")
# are treated as market-noise and excluded from editorial coverage.
_STOCK_INSTRUMENT_NEWS_RE = re.compile(
    r"חדשות\s+מניית\s*\([A-Za-z0-9._-]{2,16}\)",
    re.IGNORECASE,
)
_INVESTING_INSTRUMENT_TITLE_RE = re.compile(
    r"(?:^|\s)[A-Za-z0-9._-]{1,10}\s*\|\s*מניית\s+אינטל\b",
    re.IGNORECASE,
)
_INVESTING_PORTAL_SUFFIX_RE = re.compile(
    r"investing\.com\s*-\s*שוק\s+ההון",
    re.IGNORECASE,
)
_CALCALIST_TOPIC_TITLE_RE = re.compile(
    r"^\s*אינטל\s*-\s*כלכליסט\s*$",
    re.IGNORECASE,
)
_NON_EDITORIAL_HOST_SUFFIXES = (
    "facebook.com",
    "m.facebook.com",
    "myworkdayjobs.com",
    "workdayjobs.com",
    # Investing.com (all regional variants: il, br, en, es, …) and
    # statusinvest.com.br are ticker/BDR pages we never want in the
    # Hebrew Intel feed, even when their il. subdomain publishes
    # Hebrew wire copy. User-requested source removal (2026-04-24).
    "investing.com",
    "statusinvest.com.br",
)

# Source IDs that must never produce articles, even when the source config
# in sources.json is (or was) active. This catches stored articles whose
# GNews redirect URL never resolved to investing.com (so the host-suffix
# filter misses them) and prevents future re-ingestion regardless of
# `active` flag. User-requested (2026-04-24).
_BLOCKED_SOURCE_IDS = frozenset({
    "investing_il",
    "investing_br_itlc34",
})
_CALCALIST_TOPIC_PATH_RE = re.compile(r"^/home/0,7340,l-\d+,00\.html/?$", re.IGNORECASE)


def _title_is_ticker_list(title: str) -> bool:
    """True if title reads like a multi-stock ticker list."""
    if not title:
        return False
    return len(_TICKER_LIST_RE.findall(title)) >= 3


def _count_intel_mentions(text: str) -> int:
    """Count Intel token occurrences (standalone matches only)."""
    if not text:
        return 0
    cleaned = _clean_text(text)
    return len(INTEL_TOKEN_RE.findall(cleaned))


def _parse_any_date(value: str) -> Optional[datetime]:
    """Best-effort parser for metadata dates from article pages."""
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None

    # Common ISO variants
    try:
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass

    fmts = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
        # Hebrew-site date formats (Maariv, Ynet, Walla etc. use these in
        # visible `<time>` tags: "17:32 25/01/2026" or "25/01/2026 17:32").
        "%H:%M %d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%d.%m.%Y %H:%M",
        "%H:%M %d.%m.%Y",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(v, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue

    return None


def _extract_article_date_sync(url: str) -> Optional[str]:
    """Extract article publication date from page metadata/JSON-LD."""
    if not url:
        return None
    try:
        safe_url = html.unescape(url)
        req = urllib.request.Request(
            safe_url,
            headers={"User-Agent": "Mozilla/5.0 (IntelMediaPulse/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=12) as resp:
            html_text = resp.read(700_000).decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html_text, "lxml")

        candidates: list[str] = []
        date_meta_order = [
            ("property", "article:published_time"),
            ("property", "og:published_time"),
            ("name", "publishdate"),
            ("name", "publication_date"),
            ("name", "date"),
            ("itemprop", "datePublished"),
            ("property", "article:modified_time"),
            ("itemprop", "dateModified"),
        ]
        for attr, key in date_meta_order:
            for tag in soup.find_all("meta", attrs={attr: key}):
                val = (tag.get("content") or "").strip()
                if val:
                    candidates.append(val)

        for t in soup.find_all("time"):
            for attr in ("datetime", "content"):
                val = (t.get(attr) or "").strip()
                if val:
                    candidates.append(val)

        # JSON-LD scan (supports dict/list payloads)
        def _scan_json_ld(node):
            if isinstance(node, dict):
                for k in ("datePublished", "dateCreated", "uploadDate", "dateModified"):
                    v = node.get(k)
                    if isinstance(v, str) and v.strip():
                        candidates.append(v.strip())
                for child in node.values():
                    _scan_json_ld(child)
            elif isinstance(node, list):
                for child in node:
                    _scan_json_ld(child)

        for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = (s.string or s.get_text() or "").strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            _scan_json_ld(obj)

        for val in candidates:
            dt = _parse_any_date(val)
            if dt:
                return dt.astimezone(timezone.utc).isoformat()
    except Exception as exc:
        log.debug("Date extract failed (%s): %s", url, exc)
    return None


def _infer_legacy_date_from_url(url: str) -> Optional[str]:
    """Heuristic fallback for legacy archive URLs with missing metadata."""
    if not url:
        return None
    u = html.unescape(url).lower()

    # Globes legacy archive pages often resolve from GNews with very old did
    # ids and no parseable publication metadata in bot-facing HTML.
    if "globes.co.il" in u and "docview.aspx" in u:
        m = re.search(r"[?&]did=(\d+)", u)
        if m:
            did = int(m.group(1))
            if did < 1001400000:
                return datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()
    return None


def _is_investing_non_article_url(url: str) -> bool:
    """True for Investing listing/index/pro URLs that are not article pages."""
    if not url:
        return False
    u = html.unescape(url).strip()
    if not u:
        return False
    p = urllib.parse.urlparse(u)
    host = (p.netloc or "").lower()
    path = (p.path or "").lower().rstrip("/")
    if "investing.com" not in host:
        return False

    # Keep real article pages under /news/; reject equities listing pages.
    if path.startswith("/news/"):
        return False
    if path.startswith("/equities/") and "-news" in path:
        return True
    if path.startswith("/equities/") and re.match(r"^/equities/[a-z0-9._-]+/?$", path):
        return True
    if path.startswith("/pro/") or "/explorer" in path:
        return True
    return False


def _host_matches_suffixes(host: str, suffixes: tuple[str, ...]) -> bool:
    h = (host or "").strip().lower()
    if not h:
        return False
    return any(h == sfx or h.endswith("." + sfx) for sfx in suffixes)


def _is_non_editorial_url_or_source(url: str, resolved_url: str, source_name: str, title: str) -> bool:
    """True for social/careers/tag pages that are not editorial news articles."""
    for candidate in [resolved_url, url]:
        c = html.unescape((candidate or "").strip())
        if not c:
            continue
        p = urllib.parse.urlparse(c)
        host = (p.netloc or "").lower().strip()
        path = (p.path or "").lower().strip()
        if _host_matches_suffixes(host, _NON_EDITORIAL_HOST_SUFFIXES):
            return True
        if host.endswith("calcalist.co.il") and _CALCALIST_TOPIC_PATH_RE.match(path):
            return True

    # Source names from Google News are often plain domains (without scheme).
    src = (source_name or "").strip().lower()
    if src and "." in src and " " not in src and _host_matches_suffixes(src, _NON_EDITORIAL_HOST_SUFFIXES):
        return True

    if _CALCALIST_TOPIC_TITLE_RE.match(_clean_text(title or "")):
        return True
    return False


# Generic publisher placeholder/default images we treat as "no image" so
# enrichment will upgrade them to the real og:image. Matched as substrings
# against the full (lowercased) URL.
_PLACEHOLDER_IMAGE_PATTERNS = (
    "logo", "icon", "sprite", "avatar",
    "tmmobile", "tmdesktop",
    "/site/finance/more.jpg", "/site/more.jpg", "/images/more.jpg",
    "/more.jpg",
    "noimage", "no-image", "no_image",
    "placeholder", "default-image", "default_image",
    "generic", "fallback",
    "blank.jpg", "blank.png", "empty.jpg", "empty.png",
    "og-default", "og_default", "share-default", "share_default",
    "favicon", "apple-touch-icon",
    # Accessibility / UI widget assets that Tavily sometimes returns as the
    # page "image" instead of the real article photo.
    "close.png", "close-icon", "/pass/close",
    "nagish", "accessibility", "a11y",
    "search-icon", "menu-icon", "arrow-icon",
    # Globes UI widget directories (finance sidebar icons, share tools, etc).
    # Real article photos live on res.cloudinary.com/globes/..., not here.
    "/images/site/finance/", "/images/site2/pass/",
    "less.png", "more.png", "plus.png", "minus.png",
    "share.png", "print.png", "email.png", "comment.png",
)


def _is_weak_image_url(url: str) -> bool:
    """Heuristic for low-value images (logos/icons/placeholders/small thumbs)."""
    if not url:
        return True
    u = (url or "").strip().lower()
    if any(bad in u for bad in _PLACEHOLDER_IMAGE_PATTERNS):
        return True
    try:
        q = urllib.parse.parse_qs(urllib.parse.urlparse(u).query)
        w = int((q.get("width") or ["0"])[0])
        h = int((q.get("height") or ["0"])[0])
        if (w and w <= 360) or (h and h <= 240):
            return True
    except Exception:
        pass
    return False


def _pick_best_image_url(candidates: list[str]) -> str:
    """Rank candidate image URLs and return the best usable one."""
    best_url = ""
    best_score = -10_000
    for raw in candidates:
        c = (raw or "").strip()
        if not c:
            continue
        low = c.lower()
        score = 0
        if any(ext in low for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            score += 3
        if any(good in low for good in ["/crop_images/", "/uploads/", "/wp-content/", "/media/"]):
            score += 2
        if any(bad in low for bad in _PLACEHOLDER_IMAGE_PATTERNS):
            score -= 8
        try:
            q = urllib.parse.parse_qs(urllib.parse.urlparse(low).query)
            w = int((q.get("width") or ["0"])[0])
            h = int((q.get("height") or ["0"])[0])
            if w >= 500:
                score += 2
            elif w and w <= 360:
                score -= 4
            if h >= 280:
                score += 1
            elif h and h <= 220:
                score -= 3
        except Exception:
            pass
        if score > best_score:
            best_score = score
            best_url = c
    return best_url if best_score > -5 else ""


def _meta_mentions_intel_significantly(title: str, description: str) -> bool:
    """Significant-mention rule for title/description.

    Returns True only when Intel is a primary subject of the metadata:
      * Title contains Intel AND is not a multi-stock ticker list, OR
      * Description contains Intel >= 2 times (single tangential mention
        is not enough; those fall through to the stricter body check).
    """
    title_has_intel = _is_intel_relevant(title)
    if title_has_intel and not _title_is_ticker_list(title):
        return True
    desc_hits = _count_intel_mentions(description)
    if desc_hits >= 2:
        return True
    return False


def _hard_reject_title(title: str) -> bool:
    """True when title should be rejected regardless of body mentions.

    We treat multi-stock ticker list headlines as non-significant Intel coverage,
    even if Intel appears among many symbols.
    """
    if _title_is_ticker_list(title):
        return True
    if _STOCK_INSTRUMENT_NEWS_RE.search(title or ""):
        return True
    if _INVESTING_INSTRUMENT_TITLE_RE.search(title or ""):
        return True
    if _INVESTING_PORTAL_SUFFIX_RE.search(title or ""):
        return True
    return False


def _support_rejection_reason(article: dict) -> Optional[str]:
    """Return rejection reason code, or None if article is supported."""
    title = article.get("title", "")
    description = article.get("description", "")
    url = article.get("url", "")
    source = article.get("source", "")
    resolved_url = article.get("resolved_url", "")
    body_intel = article.get("body_intel")

    # Explicit allow-list always wins.
    if url in MANUAL_INCLUDE_URLS:
        return None

    # Hard exclude known legacy Globes archive docview URLs.
    if _infer_legacy_date_from_url(resolved_url or url):
        return "legacy_archive_url"

    # Exclude Investing index/listing/pro pages (not news articles).
    if _is_investing_non_article_url(resolved_url or url):
        return "non_article_listing_url"
    if _is_non_editorial_url_or_source(url, resolved_url, source, title):
        return "non_article_listing_url"

    # Source-level blocklist: articles tagged with a known blocked source_id
    # are rejected even if their URL never resolved out of the GNews redirect.
    source_id = (article.get("source_id") or "").strip().lower()
    if source_id in _BLOCKED_SOURCE_IDS:
        return "non_article_listing_url"

    if _hard_reject_title(title):
        return "hard_reject_title"
    if not _is_hebrew_article(title, description):
        return "non_hebrew"
    if _meta_mentions_intel_significantly(title, description):
        return None
    if body_intel is True:
        return None
    return "insufficient_intel_signal"


def _is_supported_article(article: dict) -> bool:
    """Accepted article scope with strict relevance gates."""
    return _support_rejection_reason(article) is None


# ---------------------------------------------------------------------------
# Tavily helpers
# ---------------------------------------------------------------------------

async def _tavily_search(
    client: httpx.AsyncClient,
    query: str,
    days: int = 1,
    max_results: int = 20,
) -> list[dict]:
    """Call Tavily /search; return list of result dicts. Empty list if no key."""
    if not TAVILY_API_KEY:
        return []
    try:
        payload = {
            "api_key": TAVILY_API_KEY,
            "query": query,
            "search_depth": "basic",
            "topic": "news",
            "days": days,
            "max_results": max_results,
            "include_answer": False,
        }
        resp = await client.post(
            "https://api.tavily.com/search",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as exc:
        log.warning("Tavily search failed (%s): %s", query, exc)
        return []


async def _tavily_extract(client: httpx.AsyncClient, url: str) -> dict:
    """Fetch enriched metadata for a single URL via Tavily extract."""
    if not TAVILY_API_KEY:
        return {}
    try:
        resp = await client.post(
            "https://api.tavily.com/extract",
            json={
                "api_key": TAVILY_API_KEY,
                "urls": [url],
                "extract_depth": "advanced",
                "include_images": True,
            },
            timeout=20,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            return {}
        first = results[0] or {}
        imgs = first.get("images") or []
        chosen = _pick_best_image_url([first.get("image", ""), *imgs])
        if chosen:
            first["image"] = chosen
        if not first.get("description") and first.get("raw_content"):
            first["description"] = str(first.get("raw_content", ""))[:300]
        return first
    except Exception as exc:
        log.debug("Tavily extract failed (%s): %s", url, exc)
        return {}


# ---------------------------------------------------------------------------
# Open Graph / metadata extraction (fallback when no Tavily key)
# ---------------------------------------------------------------------------

async def _fetch_body_text(client: httpx.AsyncClient, url: str) -> str:
    """Fetch page HTML and return plain-text body (capped at BODY_CHECK_CHARS)."""
    try:
        resp = await client.get(url, timeout=8, follow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
        return text[:BODY_CHECK_CHARS]
    except Exception as exc:
        log.debug("Body fetch failed (%s): %s", url, exc)
        return ""


def _body_mentions_intel_strongly(body: str) -> bool:
    """Strict body-level Intel relevance check.

    Accept only if Intel is a substantive subject, not a passing reference:
      * total keyword hits in the body >= BODY_INTEL_MIN_HITS
      * at least one keyword appears within the first BODY_INTEL_LEAD_CHARS
        characters of the text (i.e. in the article lead/intro).
    """
    if not body:
        return False
    hits = 0
    for kw in INTEL_KEYWORDS_HE + INTEL_KEYWORDS_EN:
        hits += body.count(kw)
    if hits < BODY_INTEL_MIN_HITS:
        return False
    lead = body[:BODY_INTEL_LEAD_CHARS]
    return _is_intel_relevant(lead)


def _og_extract_sync(url: str) -> dict:
    """Fetch page and parse Open Graph / meta tags — uses urllib (proxy-aware)."""
    try:
        # Use full browser-like headers. A bot-marked UA (e.g.
        # "IntelMediaPulse/1.0") gets actively blocked by Globes (TCP RST,
        # WinError 10054) and Calcalist (HTTP 403). With a real Chrome UA +
        # Accept-Language they return the article page with og:image meta.
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
                "Accept-Encoding": "identity",
            },
        )
        with urllib.request.urlopen(req, timeout=12) as resp:
            html_bytes = resp.read(500_000)  # cap at 500 KB
        html_text = html_bytes.decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html_text, "lxml")

        def meta(prop: str) -> str:
            tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
            return (tag.get("content", "") if tag else "").strip()

        def _pick_image_candidate(candidates: list[str]) -> str:
            best = ("", -10)
            for raw in candidates:
                c = (raw or "").strip()
                if not c:
                    continue
                c = urllib.parse.urljoin(url, c)
                low = c.lower()
                score = 0
                if any(ext in low for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                    score += 2
                if any(good in low for good in ["crop_images", "wp-content", "uploads", "/media/"]):
                    score += 3
                if any(bad in low for bad in ["logo", "icon", "sprite", "avatar", "newsletter", "footer", "header"]):
                    score -= 4
                if "calcalist" in low and "picserver" in low:
                    score += 3
                if score > best[1]:
                    best = (c, score)
            return best[0] if best[1] > 0 else ""

        og_image = meta("og:image") or meta("twitter:image")
        # Calcalist pages may omit OG tags in bot-facing markup but still
        # expose article images via picserver URLs inside the HTML.
        if not og_image and "calcalist.co.il" in (url or ""):
            m = re.search(
                r"https://pic1\.calcalist\.co\.il/picserver3/crop_images/[^\"'\s>]+(?:_large|_x-large)\.jpg",
                html_text,
                re.IGNORECASE,
            )
            if m:
                og_image = m.group(0)
        if not og_image:
            candidates = []
            for tag in soup.find_all("img"):
                candidates.append(tag.get("src", ""))
                candidates.append(tag.get("data-src", ""))
            for link_tag in soup.find_all("link"):
                rel = " ".join(link_tag.get("rel", [])).lower()
                if "image_src" in rel:
                    candidates.append(link_tag.get("href", ""))
            og_image = _pick_image_candidate(candidates)

        return {
            "og_title": meta("og:title") or (soup.title.string.strip() if soup.title else ""),
            "og_description": meta("og:description") or meta("description"),
            "og_image": og_image,
            "og_site_name": meta("og:site_name"),
        }
    except Exception as exc:
        log.debug("OG extract failed (%s): %s", url, exc)
        return {}


async def _og_extract(client: httpx.AsyncClient, url: str) -> dict:
    """Fetch page and parse Open Graph — delegates to sync urllib version via executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_THREAD_POOL, _og_extract_sync, url)


async def _extract_article_date(url: str) -> Optional[str]:
    """Run source-date extraction off the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_THREAD_POOL, _extract_article_date_sync, url)


async def _resolve_gnews_url_async(url: str, source_url: str = "") -> str:
    """Resolve Google News redirect URLs off the event loop (blocking I/O)."""
    loop = asyncio.get_event_loop()

    def _resolve() -> str:
        return _GNEWS_RESOLVER.resolve(url, source_url=source_url) or ""

    return await loop.run_in_executor(_THREAD_POOL, _resolve)


async def _enrich_article(
    client: httpx.AsyncClient,
    article: dict,
    cache: dict,
) -> dict:
    """
    Enrich an article dict with image/description.
    Uses cached result if available; writes to cache on success.
    """
    url = article.get("url", "")
    uid = _url_id(url)

    if uid in cache:
        # `article_cache.json` is shared with body-check data (`body_intel`).
        # Only merge real enrichment fields and continue when image is still missing.
        cached = cache[uid]
        for key in ("image", "description", "resolved_url"):
            val = cached.get(key)
            if not val:
                continue
            if key == "image":
                # Only trust cached image if it is actually usable.
                # Weak/placeholder cached values must not overwrite and must
                # not block a fresh OG fetch below.
                if _is_weak_image_url(val):
                    continue
                if (not article.get("image")) or _is_weak_image_url(article.get("image", "")):
                    article["image"] = val
            elif not article.get(key):
                article[key] = val
        # If we already have both key enrichment fields, skip network work.
        if article.get("image") and article.get("description") and not _is_weak_image_url(article.get("image", "")):
            return article

    enriched: dict = {}

    # Resolve Google News redirect URLs before enrichment (applies to both
    # Tavily and OG modes).
    enrich_url = url
    if "news.google.com" in url:
        resolved = await _resolve_gnews_url_async(url, source_url=article.get("source_url", ""))
        if resolved:
            enrich_url = resolved
            if "resolved_url" not in article:
                article["resolved_url"] = resolved

    # Strip URL fragment (e.g. "#utm_source=RSS" appended by feeds) — some
    # publisher pages choke on fragments and serve a shell page without
    # og:image meta. The fragment is irrelevant for fetching the page.
    if "#" in enrich_url:
        enrich_url = enrich_url.split("#", 1)[0]

    if TAVILY_API_KEY:
        # Strategy: OG is the authoritative source for article images
        # (publisher-controlled og:image meta). Tavily frequently returns
        # UI widget icons (close/share/less/more/nagish) instead of the real
        # article photo, so we never trust its image blindly. Run OG first
        # and fall back to Tavily's image only if OG fails *and* Tavily's
        # image passes the weak-image check. Tavily is still useful for
        # description/content extraction.
        og = await _og_extract(client, enrich_url)
        og_img = og.get("og_image", "")
        image_choice = og_img if (og_img and not _is_weak_image_url(og_img)) else ""

        raw = await _tavily_extract(client, enrich_url)
        if not image_choice:
            tavily_image = raw.get("image", "")
            if tavily_image and not _is_weak_image_url(tavily_image):
                image_choice = tavily_image

        description = (
            raw.get("description")
            or raw.get("content", "")[:300]
            or og.get("og_description", "")
        )
        enriched = {"image": image_choice, "description": description}
    else:
        og = await _og_extract(client, enrich_url)
        og_img = og.get("og_image", "")
        enriched = {
            "image": og_img if (og_img and not _is_weak_image_url(og_img)) else "",
            "description": og.get("og_description", ""),
        }

    # Cache and merge — UPDATE existing cache entry rather than overwrite,
    # to preserve verified_published_at, date_checked_at, body_intel, etc.
    if enriched.get("image") or enriched.get("description"):
        existing = cache.get(uid, {}) if isinstance(cache.get(uid), dict) else {}
        for k, v in enriched.items():
            if v:  # don't overwrite with empty values
                existing[k] = v
        cache[uid] = existing
        if enriched.get("image") and ((not article.get("image")) or _is_weak_image_url(article.get("image", ""))):
            article["image"] = enriched.get("image", "")
        if enriched.get("description") and not article.get("description"):
            article["description"] = enriched.get("description", "")

    return article


# ---------------------------------------------------------------------------
# Google News RSS ingestion
# ---------------------------------------------------------------------------

def _parse_gnews_feed_sync(feed_url: str, source_map: dict[str, dict]) -> list[dict]:
    """Parse a Google News RSS feed and return article dicts (sync, call via executor)."""
    parsed = _parse_feed_with_timeout(feed_url)

    articles = []
    for entry in parsed.entries:
        raw_summary = entry.get("summary", "")
        summary_image = _extract_image_from_html(raw_summary)
        # Primary: extract real URL from summary HTML (contains <a href="real-url">)
        url = _extract_url_from_gnews_summary(raw_summary)
        # Fallback: try base64 decode of the gnews redirect link
        if not url:
            url = _decode_gnews_url(entry.get("link", ""))
        # Last resort: keep the gnews link (enrichment/body-check will be skipped)
        if not url:
            url = entry.get("link", "")
        # Capture source domain from feedparser source element (for image enrichment)
        source_href = (entry.get("source") or {}).get("href", "")
        title = _clean_text(entry.get("title", ""))
        description = _clean_text(raw_summary)
        pub = entry.get("published_parsed") or entry.get("updated_parsed")
        published_at = (
            datetime(*pub[:6], tzinfo=timezone.utc).isoformat()
            if pub
            else datetime.now(timezone.utc).isoformat()
        )

        # Google News uses the outlet name in the title field as "Title - Outlet"
        source_name = ""
        if " - " in title:
            parts = title.rsplit(" - ", 1)
            title = parts[0].strip()
            source_name = parts[1].strip()

        # Relevance check: Hebrew required; Intel must be mentioned
        # significantly in title/description, otherwise the article goes
        # through the stricter body check (≥5 body hits + lead mention).
        if not _is_hebrew_article(title, description):
            continue
        if _hard_reject_title(title):
            continue
        relevant_meta = _meta_mentions_intel_significantly(title, description)

        # Match to known source
        source_id = _match_source(source_name, url, source_map)

        # Only allow body-check fallback for articles tied to a known
        # monitored source — keeps the scan tight and avoids the whole web.
        known_source = source_id in source_map
        if not relevant_meta and not known_source:
            continue

        articles.append({
            "id": _url_id(url),
            "url": url,
            "title": title,
            "description": description,
            "source": source_name,
            "source_id": source_id,
            "published_at": published_at,
            "image": summary_image,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "via": "google_news_rss",
            "pending_body_check": not relevant_meta,
            "source_url": source_href,  # real source domain for image enrichment
        })

    return articles


def _match_source(source_name: str, url: str, source_map: dict[str, dict]) -> str:
    """Try to match a source name/URL to a known source id."""
    if not source_name and not url:
        return "unknown"

    name_lower = (source_name or "").lower().strip()
    url_lower = (url or "").lower()

    for sid, src in source_map.items():
        display = (src.get("display_name") or "").lower().strip()
        name_he = (src.get("name_he") or "").lower().strip()
        name_en = (src.get("name") or "").lower().strip()
        extra_aliases = [str(a).lower().strip() for a in (src.get("aliases") or [])]
        aliases = [display, name_he, name_en, *extra_aliases]
        domains = src.get("domains") or []
        if src.get("domain"):
            domains = list(dict.fromkeys([src.get("domain"), *domains]))

        for domain in domains:
            norm_domain = (domain or "").lower().replace("www.", "")
            if norm_domain and norm_domain in url_lower:
                return sid

        if name_lower:
            for alias in aliases:
                if not alias:
                    continue
                # Bidirectional substring match for Google News short outlet
                # names (e.g. "היום" ↔ "ישראל היום"). Require alias ≥ 3 chars
                # to avoid false positives.
                if alias in name_lower:
                    return sid
                if len(alias) >= 3 and name_lower in alias:
                    return sid

    parsed_domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
    if parsed_domain:
        for sid, src in source_map.items():
            domains = src.get("domains") or []
            if src.get("domain"):
                domains = list(dict.fromkeys([src.get("domain"), *domains]))
            normalized = [(d or "").lower().replace("www.", "") for d in domains]
            if parsed_domain in normalized:
                return sid

    return source_name.lower().replace(" ", "_") if source_name else (parsed_domain.replace('.', '_') if parsed_domain else "unknown")


# ---------------------------------------------------------------------------
# Tavily news search ingestion
# ---------------------------------------------------------------------------

async def _collect_from_tavily(
    client: httpx.AsyncClient, source_map: dict[str, dict]
) -> list[dict]:
    """Run Tavily news searches and return article dicts."""
    if not TAVILY_API_KEY:
        return []

    queries = [
        "Intel Israel",
        "אינטל",
        "Intel chip semiconductor",
    ]
    all_results: list[dict] = []

    for q in queries:
        results = await _tavily_search(client, q, days=1, max_results=20)
        all_results.extend(results)

    articles = []
    seen_urls: set[str] = set()

    for r in all_results:
        url = r.get("url", "")
        if not url or url in seen_urls:
            continue
        title = _clean_text(r.get("title", ""))
        description = _clean_text(r.get("content", ""))[:300]
        published_date = r.get("published_date", "")
        source_name = r.get("source", urllib.parse.urlparse(url).netloc)

        if not _is_hebrew_article(title, description):
            continue
        if _hard_reject_title(title):
            continue
        if not _meta_mentions_intel_significantly(title, description):
            continue

        seen_urls.add(url)
        source_id = _match_source(source_name, url, source_map)

        # Parse date
        pub_dt = _parse_date(published_date) or datetime.now(timezone.utc)

        articles.append({
            "id": _url_id(url),
            "url": url,
            "title": title,
            "description": description,
            "source": source_name,
            "source_id": source_id,
            "published_at": pub_dt.isoformat(),
            "image": r.get("image", ""),
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "via": "tavily",
        })

    return articles


def _parse_date(date_str: str) -> Optional[datetime]:
    """Try to parse a date string into an aware datetime."""
    if not date_str:
        return None
    raw = str(date_str).strip()

    # Fast path for common ISO-8601 variants, including microseconds and +00:00.
    try:
        iso = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass

    formats = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Direct RSS feed ingestion
# ---------------------------------------------------------------------------

def _collect_from_rss_sync(sources: list[dict]) -> list[dict]:
    """Parse RSS feeds for all active sources that have rss_url (sync, call via executor)."""
    articles = []
    for src in sources:
        if not src.get("active") or not src.get("rss_url"):
            continue
        feed_url = src["rss_url"]
        parsed = _parse_feed_with_timeout(feed_url)

        for entry in parsed.entries:
            url = entry.get("link", "")
            title = _clean_text(entry.get("title", ""))
            raw_summary = entry.get("summary", "")
            description = _clean_text(raw_summary)
            summary_image = _extract_image_from_html(raw_summary)
            pub = entry.get("published_parsed") or entry.get("updated_parsed")
            published_at = (
                datetime(*pub[:6], tzinfo=timezone.utc).isoformat()
                if pub
                else datetime.now(timezone.utc).isoformat()
            )

            if not url:
                continue
            if not _is_hebrew_article(title, description):
                continue
            if _hard_reject_title(title):
                continue
            relevant_meta = _meta_mentions_intel_significantly(title, description)
            # Direct-RSS articles are always from a monitored source, so we
            # can safely defer relevance verification to a body check.

            articles.append({
                "id": _url_id(url),
                "url": url,
                "title": title,
                "description": description[:400],
                "source": src.get("display_name", src.get("name", "")),
                "source_id": src.get("id", ""),
                "published_at": published_at,
                "image": summary_image,
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "via": "rss",
                "pending_body_check": not relevant_meta,
            })

    return articles


def _collect_manual_overrides_sync(source_map: dict[str, dict]) -> list[dict]:
    """Fetch high-priority manual URLs and force include them if reachable."""
    articles: list[dict] = []
    for url in sorted(MANUAL_INCLUDE_URLS):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (IntelMediaPulse/1.0)"},
            )
            with urllib.request.urlopen(req, timeout=12) as resp:
                html_bytes = resp.read(350_000)
            html_text = html_bytes.decode("utf-8", errors="ignore")
            soup = BeautifulSoup(html_text, "lxml")
            title = _clean_text(soup.title.string.strip() if soup.title and soup.title.string else "")
            desc_tag = (
                soup.find("meta", attrs={"name": "description"})
                or soup.find("meta", attrs={"property": "og:description"})
            )
            description = _clean_text(desc_tag.get("content", "") if desc_tag else "")
            source_name = urllib.parse.urlparse(url).netloc
            source_id = _match_source(source_name, url, source_map)
            og = _og_extract_sync(url)
            articles.append(
                {
                    "id": _url_id(url),
                    "url": url,
                    "title": title,
                    "description": description[:400],
                    "source": source_name,
                    "source_id": source_id,
                    "published_at": datetime.now(timezone.utc).isoformat(),
                    "image": og.get("og_image", ""),
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "via": "manual_override",
                    "body_intel": True,
                }
            )
        except Exception as exc:
            log.debug("Manual include fetch failed (%s): %s", url, exc)
    return articles


# ---------------------------------------------------------------------------
# Source discovery (Google News + Tavily, 180-day window)
# ---------------------------------------------------------------------------

async def discover_sources() -> None:
    """Discover new sources from Google News RSS and Tavily; merge into sources.json."""
    log.info("Starting source discovery …")
    sources_data = _read_json(SOURCES_FILE, {"sources": [], "_meta": {}})
    existing_sources = sources_data.get("sources", [])
    known_domains = set()
    for source in existing_sources:
        if source.get("domain"):
            known_domains.add(source["domain"])
        for domain in source.get("domains", []):
            if domain:
                known_domains.add(domain)

    new_sources: list[dict] = []

    async with httpx.AsyncClient(
        headers={"User-Agent": "IntelMediaPulse/1.0 (+bot)"},
        follow_redirects=True,
        proxies=_INTEL_PROXY,
    ) as client:
        # Tavily discovery over 180 days
        if TAVILY_API_KEY:
            for query in ["אינטל", "Intel Israel", "Intel"]:
                results = await _tavily_search(client, query, days=180, max_results=20)
                for r in results:
                    url = r.get("url", "")
                    if not url:
                        continue
                    domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                    if domain and domain not in known_domains:
                        known_domains.add(domain)
                        new_sources.append({
                            "id": domain.replace(".", "_"),
                            "name": r.get("source", domain),
                            "name_he": r.get("source", domain),
                            "display_name": r.get("source", domain),
                            "domain": domain,
                            "domains": [domain],
                            "rss_url": None,
                            "color": "#6b7280",
                            "category": "discovered",
                            "country": "unknown",
                            "language": "unknown",
                            "active": True,
                            "discovery_method": "tavily",
                        })

        # Google News RSS discovery
        loop2 = asyncio.get_event_loop()
        for q_enc in [
            urllib.parse.quote("אינטל"),
            urllib.parse.quote("Intel Israel"),
            urllib.parse.quote("Intel"),
        ]:
            feed_url = GNEWS_RSS_BASE.format(query=q_enc)
            try:
                parsed = await loop2.run_in_executor(
                    _THREAD_POOL, _parse_feed_with_timeout, feed_url
                )
                for entry in parsed.entries:
                    url = entry.get("link", "")
                    title = _clean_text(entry.get("title", ""))
                    summary = _clean_text(entry.get("summary", ""))
                    if not _is_hebrew_article(title, summary):
                        continue
                    if not _is_intel_relevant(title):
                        continue
                    domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                    if domain and domain not in known_domains:
                        known_domains.add(domain)
                        source_name = title.rsplit(" - ", 1)[-1].strip() if " - " in title else domain
                        new_sources.append({
                            "id": domain.replace(".", "_"),
                            "name": source_name,
                            "name_he": source_name,
                            "display_name": source_name,
                            "domain": domain,
                            "domains": [domain],
                            "rss_url": None,
                            "color": "#6b7280",
                            "category": "discovered",
                            "country": "unknown",
                            "language": "unknown",
                            "active": True,
                            "discovery_method": "google_news",
                        })
            except Exception as exc:
                log.warning("GNews discovery failed: %s", exc)

    if new_sources:
        existing_sources.extend(new_sources)
        sources_data["sources"] = existing_sources
        sources_data["_meta"]["last_discovery"] = datetime.now(timezone.utc).isoformat()
        _write_json(SOURCES_FILE, sources_data)
        log.info("Source discovery added %d new sources", len(new_sources))
    else:
        log.info("Source discovery: no new sources found")


# ---------------------------------------------------------------------------
# Main article collection job
# ---------------------------------------------------------------------------

async def collect_articles() -> None:
    """
    Collect articles from all sources.
    1. Google News RSS (queries: אינטל, Intel Israel, Intel)
    2. Source RSS feeds
    3. Tavily news search (if key present)
    4. Enrich missing images/descriptions
    5. Dedupe by URL; save to articles.json
    """
    if _refresh_status["running"]:
        log.info("collect_articles: already running, skipping")
        return

    _refresh_status["running"] = True
    _refresh_status["last_error"] = None
    start_ts = time.monotonic()

    try:
        sources = _load_sources()
        source_map = {s["id"]: s for s in sources}

        all_new: list[dict] = []
        pre_bodycheck_candidates = 0
        body_check_rejected_count = 0
        support_drop_reasons: Counter[str] = Counter()

        # 1. Google News RSS (run in thread pool to avoid blocking event loop)
        loop = asyncio.get_event_loop()
        gnews_tasks = []
        # Global queries
        for q in ["אינטל", "Intel Israel", "Intel"]:
            q_enc = urllib.parse.quote(q)
            feed_url = GNEWS_RSS_BASE.format(query=q_enc)
            gnews_tasks.append(
                loop.run_in_executor(
                    _THREAD_POOL,
                    _parse_gnews_feed_sync,
                    feed_url,
                    source_map,
                )
            )
        # Per-source site-scoped queries — catches articles where Intel appears
        # only in the body (Google News full-text indexing).
        # All 67 sources ARE monitored: via direct RSS feeds + global GNews queries.
        # Site-scoped GNews queries are limited to PRIORITY sources only (capped
        # at SITE_QUERY_LIMIT, default 20) to prevent Google throttling.
        # Non-priority sources rely on the global GNews queries + their RSS feed.
        active_sources_with_domain = [
            s for s in sources
            if s.get("active", True) and (s.get("domain") or "").strip()
        ]
        site_query_limit_env = os.getenv("SITE_QUERY_LIMIT", "").strip()
        SITE_QUERY_LIMIT = (
            int(site_query_limit_env)
            if site_query_limit_env
            else 20  # Default: priority sources only to prevent throttling
        )
        # Prioritize finance/flagship sources where misses are more painful.
        PRIORITY_DOMAIN_ORDER = [
            "globes.co.il",
            "bizportal.co.il",
            "calcalist.co.il",
            "calcalistech.com",
            "ctech.com",
            "themarker.com",
            "ice.co.il",
            "ynet.co.il",
        ]
        site_query_count = 0
        domain_priority = {d: i for i, d in enumerate(PRIORITY_DOMAIN_ORDER)}
        sorted_sources = sorted(
            sources,
            key=lambda s: domain_priority.get((s.get("domain") or "").strip().lower(), 9999),
        )
        for src in sorted_sources:
            if site_query_count >= SITE_QUERY_LIMIT:
                break
            if not src.get("active", True):
                continue
            domain = (src.get("domain") or "").strip()
            if not domain:
                continue
            site_query_count += 1
            # Keep one site query per domain to avoid opening hundreds of
            # concurrent sockets in flaky networks.
            q_enc = urllib.parse.quote(f"אינטל site:{domain}")
            feed_url = GNEWS_RSS_BASE.format(query=q_enc)
            gnews_tasks.append(
                loop.run_in_executor(
                    _THREAD_POOL,
                    _parse_gnews_feed_sync,
                    feed_url,
                    source_map,
                )
            )

        _refresh_status["source_monitoring"] = {
            "configured_site_query_limit": SITE_QUERY_LIMIT,
            "active_sources_total": sum(1 for s in sources if s.get("active", True)),
            "active_sources_with_domain": len(active_sources_with_domain),
            "site_queries_scheduled": site_query_count,
            "note": "All sources monitored via RSS+global queries; site-scoped GNews limited to priority sources.",
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        # Explicit extra domains for high-value finance sources that may be
        # underrepresented in general Intel queries.
        for domain in SITE_SCOPED_EXTRA_DOMAINS:
            for q in ["אינטל", "Intel"]:
                q_enc = urllib.parse.quote(f"{q} site:{domain}")
                feed_url = GNEWS_RSS_BASE.format(query=q_enc)
                gnews_tasks.append(
                    loop.run_in_executor(
                        _THREAD_POOL,
                        _parse_gnews_feed_sync,
                        feed_url,
                        source_map,
                    )
                )
        gnews_results = await asyncio.gather(*gnews_tasks, return_exceptions=True)
        for r in gnews_results:
            if isinstance(r, list):
                all_new.extend(r)

        # 2. Direct RSS feeds (thread pool)
        rss_articles = await loop.run_in_executor(
            _THREAD_POOL, _collect_from_rss_sync, sources
        )
        all_new.extend(rss_articles)

        # 2b. HTML fallback for selected sources where RSS/GNews are flaky.
        html_fallback_articles = await loop.run_in_executor(
            _THREAD_POOL, _collect_from_html_fallback_sync, sources
        )
        all_new.extend(html_fallback_articles)

        # 2c. Manual include overrides for must-track URLs.
        manual_override_articles = await loop.run_in_executor(
            _THREAD_POOL, _collect_manual_overrides_sync, source_map
        )
        all_new.extend(manual_override_articles)

        # Load cache before the HTTP client block so it's available for both
        # URL resolution and the merge step that follows.
        cache = _load_cache()

        # 3. Tavily
        async with httpx.AsyncClient(
            headers={"User-Agent": "IntelMediaPulse/1.0"},
            follow_redirects=True,
            proxies=_INTEL_PROXY,
        ) as client:
            tavily_articles = await _collect_from_tavily(client, source_map)
            all_new.extend(tavily_articles)

            # 3b. Body-check pass: for candidates whose title/description don't
            # mention Intel, fetch the article body and keep only those whose
            # body mentions Intel. Results are cached so we don't re-fetch.
            pre_bodycheck_candidates = len(all_new)
            pending = [a for a in all_new if a.get("pending_body_check")]
            pending = pending[:BODY_CHECK_LIMIT]

            # Split into cached (already decided) and to-fetch
            confirmed_ids: set[str] = set()
            rejected_ids: set[str] = set()
            to_fetch: list[dict] = []
            for a in pending:
                uid = a["id"]
                entry = cache.get(uid, {})
                verdict = entry.get("body_intel")
                if verdict is True:
                    confirmed_ids.add(uid)
                elif verdict is False:
                    rejected_ids.add(uid)
                else:
                    to_fetch.append(a)

            async def _check_one(art: dict) -> tuple[str, bool]:
                body = await _fetch_body_text(client, art["url"])
                return art["id"], _body_mentions_intel_strongly(body)

            if to_fetch:
                log.info("Body-check: scanning %d articles", len(to_fetch))
                # Limit concurrency to avoid hammering publishers
                sem = asyncio.Semaphore(6)

                async def _bounded(art):
                    async with sem:
                        return await _check_one(art)

                results = await asyncio.gather(
                    *[_bounded(a) for a in to_fetch],
                    return_exceptions=True,
                )
                for res in results:
                    if isinstance(res, tuple):
                        uid, is_intel = res
                        cache.setdefault(uid, {})["body_intel"] = is_intel
                        (confirmed_ids if is_intel else rejected_ids).add(uid)

            # Keep: not pending, OR pending-and-confirmed. Drop pending-rejected.
            before = len(all_new)
            all_new = [
                a for a in all_new
                if not a.get("pending_body_check") or a["id"] in confirmed_ids
            ]
            # Clear the flag on survivors
            for a in all_new:
                a.pop("pending_body_check", None)
            body_check_rejected_count = len(rejected_ids)
            log.info(
                "Body-check: kept %d / %d (confirmed=%d, rejected=%d)",
                len(all_new), before, len(confirmed_ids), len(rejected_ids),
            )

            # 4. Enrich
            enriched: list[dict] = []
            # Enrich articles missing image, prioritize GNews items that carry
            # source_url so we can at least fetch publisher-level OG images.
            needs_enrich = [a for a in all_new if (not a.get("image")) or _is_weak_image_url(a.get("image", ""))]
            needs_enrich.sort(
                key=lambda a: 0
                if ("news.google.com" in (a.get("url") or "") and a.get("source_url"))
                else 1
            )
            needs_enrich = needs_enrich[:ENRICH_LIMIT]
            enrichment_tasks = [_enrich_article(client, a, cache) for a in needs_enrich]
            enriched = await asyncio.gather(*enrichment_tasks, return_exceptions=True)
            # Replace enriched articles back
            enrich_map = {}
            for res in enriched:
                if isinstance(res, dict):
                    enrich_map[res["id"]] = res
            all_new = [enrich_map.get(a["id"], a) for a in all_new]
            _save_cache(cache)

        # 5. Merge with existing articles (dedupe by URL id)
        existing_raw = [a for a in _load_articles() if _is_supported_article(a)]
        # Decode any legacy gnews URLs in the existing store so they compare
        # correctly against the newly-collected articles (which are already decoded).
        for a in existing_raw:
            if "news.google.com" in a.get("url", ""):
                real = _decode_gnews_url(a["url"])
                if real != a["url"]:
                    a["url"] = real
                    a["id"] = _url_id(real)
        existing = [_sanitize_article(a) for a in existing_raw]
        all_new = [_sanitize_article(a) for a in all_new]
        existing_ids = {a["id"] for a in existing}
        new_unique = [a for a in all_new if a["id"] not in existing_ids]

        merged = existing + new_unique
        supported_merged: list[dict] = []
        for a in merged:
            reason = _support_rejection_reason(a)
            if reason is None:
                supported_merged.append(_sanitize_article(a))
            else:
                support_drop_reasons[reason] += 1
        merged = supported_merged

        deduped_merged = []
        seen_ids = set()
        seen_urls = set()
        canonical_index: dict[str, int] = {}
        signature_index: dict[str, int] = {}
        # Looser, timestamp-tolerant key for duplicates coming from GNews
        # (which often reports a different `published_at` than the publisher's
        # direct RSS feed — sometimes by hours).
        day_signature_index: dict[str, int] = {}
        # Strongest signal for near-identical GNews republishes: exact same
        # source_id + exact same published_at ISO timestamp + first title word
        # in common. Catches title variants that differ by one Hebrew verb
        # form (e.g. "מכה" vs "היכתה") that day-signature misses.
        timestamp_signature_index: dict[str, int] = {}

        def _day_sig(article: dict) -> str:
            title = _clean_text(article.get("title", "")).strip().lower()
            source = (article.get("source_id") or article.get("source") or "").strip().lower()
            pub = (article.get("published_at") or "").strip()
            day = pub[:10] if len(pub) >= 10 else ""
            if not title or not source or not day:
                return ""
            return f"{title}||{source}||{day}"

        def _timestamp_sig(article: dict) -> str:
            title = _clean_text(article.get("title", "")).strip().lower()
            source = (article.get("source_id") or article.get("source") or "").strip().lower()
            pub = (article.get("published_at") or "").strip()
            if not title or not source or not pub:
                return ""
            first_word = title.split()[0] if title.split() else ""
            if len(first_word) < 3:
                return ""
            return f"{first_word}||{source}||{pub}"

        def _article_quality_score(a: dict) -> int:
            score = 0
            u = (a.get("url") or "").lower()
            if u and "news.google.com" not in u:
                score += 4
            if (a.get("image") or "") and not _is_weak_image_url(a.get("image", "")):
                score += 2
            if len((a.get("description") or "").strip()) >= 60:
                score += 1
            if a.get("resolved_url"):
                score += 1
            via = (a.get("via") or "").lower()
            if via == "rss":
                score += 1
            if via == "html_fallback":
                score += 1
            return score

        for article in merged:
            canonical_url = _canonical_article_url(article)
            article_url = article.get("url") or ""
            if canonical_url:
                article["canonical_url"] = canonical_url
                # Prefer canonical direct publisher URL over gnews redirect.
                if "news.google.com" in article_url.lower() and "news.google.com" not in canonical_url.lower():
                    article["url"] = canonical_url
                    article["id"] = _url_id(canonical_url)

            article_id = article.get("id")
            article_url = article.get("url")
            title_sig = _clean_text(article.get("title", "")).strip().lower()
            source_sig = (article.get("source_id") or article.get("source") or "").strip().lower()
            published_sig = (article.get("published_at") or "").strip()
            dedupe_sig = f"{title_sig}||{source_sig}||{published_sig}" if title_sig and source_sig and published_sig else ""

            # Canonical dedupe comes first; if a duplicate exists, keep the
            # higher-quality representation.
            if canonical_url and canonical_url in canonical_index:
                prev_idx = canonical_index[canonical_url]
                prev_article = deduped_merged[prev_idx]
                if _article_quality_score(article) > _article_quality_score(prev_article):
                    deduped_merged[prev_idx] = article
                continue

            # Fallback: same headline from same source at same timestamp.
            if dedupe_sig and dedupe_sig in signature_index:
                prev_idx = signature_index[dedupe_sig]
                prev_article = deduped_merged[prev_idx]
                if _article_quality_score(article) > _article_quality_score(prev_article):
                    deduped_merged[prev_idx] = article
                continue

            # Looser fallback: same headline + same source, same calendar day.
            # Catches GNews vs direct-RSS variants whose `published_at` drifts
            # by a few hours.
            day_sig = _day_sig(article)
            if day_sig and day_sig in day_signature_index:
                prev_idx = day_signature_index[day_sig]
                prev_article = deduped_merged[prev_idx]
                if _article_quality_score(article) > _article_quality_score(prev_article):
                    deduped_merged[prev_idx] = article
                continue

            # Strongest cross-variant fallback: same source + exact published_at
            # + same first title word. Catches title-verb variants (מכה/היכתה)
            # which day-sig misses because titles differ.
            ts_sig = _timestamp_sig(article)
            if ts_sig and ts_sig in timestamp_signature_index:
                prev_idx = timestamp_signature_index[ts_sig]
                prev_article = deduped_merged[prev_idx]
                if _article_quality_score(article) > _article_quality_score(prev_article):
                    deduped_merged[prev_idx] = article
                continue

            if article_id and article_id in seen_ids:
                continue
            if article_url and article_url in seen_urls:
                continue

            if article_id:
                seen_ids.add(article_id)
            if article_url:
                seen_urls.add(article_url)
            deduped_merged.append(article)
            if canonical_url:
                canonical_index[canonical_url] = len(deduped_merged) - 1
            if dedupe_sig:
                signature_index[dedupe_sig] = len(deduped_merged) - 1
            if day_sig:
                day_signature_index[day_sig] = len(deduped_merged) - 1
            if ts_sig:
                timestamp_signature_index[ts_sig] = len(deduped_merged) - 1

        # Backfill source_url for legacy Google News items (older entries may
        # not have it, which blocks image enrichment on gnews redirect URLs).
        for a in deduped_merged:
            if "news.google.com" not in (a.get("url") or ""):
                continue
            if a.get("source_url"):
                continue
            sid = a.get("source_id") or ""
            src = source_map.get(sid, {})
            domain = (src.get("domain") or "").strip()
            if domain:
                a["source_url"] = f"https://{domain}"

        # Backfill enrichment for existing articles so image data can improve
        # even on runs with little or no new content.
        backfill_candidates = [
            a for a in deduped_merged
            if (not a.get("image")) or _is_weak_image_url(a.get("image", ""))
        ]
        backfill_candidates.sort(
            key=lambda a: 0
            if ("news.google.com" in (a.get("url") or "") and a.get("source_url"))
            else 1
        )
        backfill_candidates = backfill_candidates[:ENRICH_LIMIT]
        if backfill_candidates:
            async with httpx.AsyncClient(
                headers={"User-Agent": "IntelMediaPulse/1.0"},
                follow_redirects=True,
                proxies=_INTEL_PROXY,
            ) as client:
                backfilled = await asyncio.gather(
                    *[_enrich_article(client, a, cache) for a in backfill_candidates],
                    return_exceptions=True,
                )
            backfill_map = {a["id"]: a for a in backfilled if isinstance(a, dict)}
            deduped_merged = [backfill_map.get(a["id"], a) for a in deduped_merged]
            _save_cache(cache)

        merged = deduped_merged

        # Comprehensive date verification from source pages. We cache the
        # verified date per article id to avoid re-fetching every run.
        verify_targets = merged[:DATE_VERIFY_MAX]
        if verify_targets:
            verify_sem = asyncio.Semaphore(6)
            now_utc = datetime.now(timezone.utc)

            async def _verify_one_date(a: dict):
                uid = a.get("id") or _url_id(a.get("url", ""))
                entry = cache.setdefault(uid, {})
                last_checked = _parse_any_date(entry.get("date_checked_at", ""))
                has_verified = bool(entry.get("verified_published_at"))

                # Re-verify aggressively for recently-published articles:
                # the RSS pubDate can be wrong on republishes (GNews re-
                # surfaces old articles with a "now" pubDate). For items
                # younger than 7 days of reported age, ignore the 24h cache
                # window and re-fetch the publisher date.
                reported_dt = _parse_any_date(a.get("published_at", ""))
                is_recent = (
                    reported_dt is not None
                    and (now_utc - reported_dt) < timedelta(days=7)
                )

                if (
                    has_verified
                    and last_checked
                    and (now_utc - last_checked) < timedelta(hours=DATE_VERIFY_RECHECK_HOURS)
                    and not is_recent
                ):
                    return

                target_url = a.get("resolved_url") or a.get("url") or ""
                if not target_url:
                    entry["date_checked_at"] = now_utc.isoformat()
                    return

                if "news.google.com" in target_url:
                    try:
                        resolved_target = await _resolve_gnews_url_async(
                            target_url,
                            source_url=a.get("source_url", ""),
                        )
                        if resolved_target:
                            target_url = resolved_target
                    except Exception:
                        pass

                async with verify_sem:
                    verified = await _extract_article_date(target_url)
                if not verified:
                    verified = _infer_legacy_date_from_url(target_url)

                if verified:
                    # Trust verified date only when it is REASONABLY close
                    # to the RSS-reported date. Many publishers (Globes,
                    # Bizportal, etc.) emit a stale/template og:date that
                    # points to ancient years (e.g. 2007). When the drift
                    # exceeds 7 days, prefer the RSS date and discard the
                    # bogus publisher meta to avoid losing fresh articles.
                    verified_dt = _parse_any_date(verified)
                    drift_too_large = (
                        reported_dt is not None
                        and verified_dt is not None
                        and abs((reported_dt - verified_dt).total_seconds()) > 7 * 24 * 3600
                    )
                    if drift_too_large:
                        log.info(
                            "Date verify drift >7d on %s: rss=%s publisher=%s -- keeping RSS date",
                            target_url,
                            a.get("published_at"),
                            verified,
                        )
                        # Do NOT cache the bogus verified date.
                    else:
                        entry["verified_published_at"] = verified
                entry["date_checked_at"] = now_utc.isoformat()

            await asyncio.gather(*[_verify_one_date(a) for a in verify_targets], return_exceptions=True)
            _save_cache(cache)

        # Apply verified publication date when available.
        for a in merged:
            uid = a.get("id") or _url_id(a.get("url", ""))
            verified = (cache.get(uid) or {}).get("verified_published_at")
            if verified:
                a["published_at"] = verified

        # Prune articles older than CACHE_MAX_DAYS.
        # If published_at is missing/unparseable, fall back to collected_at.
        # If neither date parses, drop the article.
        cutoff = datetime.now(timezone.utc) - timedelta(days=CACHE_MAX_DAYS)
        pruned = []
        for a in merged:
            dt = _parse_date(a.get("published_at", "")) or _parse_date(a.get("collected_at", ""))
            if dt and dt >= cutoff:
                pruned.append(a)
        merged = pruned

        # Sort descending by publication date
        def _sort_key(a):
            dt = _parse_date(a.get("published_at", ""))
            return dt.isoformat() if dt else ""

        merged.sort(key=_sort_key, reverse=True)

        image_present_count = sum(1 for a in merged if a.get("image"))
        weak_image_count = sum(
            1 for a in merged if a.get("image") and _is_weak_image_url(a.get("image", ""))
        )
        quality_report = {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "collection": {
                "pre_bodycheck_candidates": pre_bodycheck_candidates,
                "post_bodycheck_candidates": len(all_new),
                "body_check_rejected": body_check_rejected_count,
                "new_unique": len(new_unique),
                "merged_total": len(merged),
            },
            "images": {
                "with_image": image_present_count,
                "without_image": max(len(merged) - image_present_count, 0),
                "weak_image": weak_image_count,
                "coverage_pct": round((image_present_count / len(merged) * 100), 1) if merged else 0.0,
            },
            "filtering": {
                "drop_reasons": dict(support_drop_reasons),
            },
            "source_monitoring": dict(_refresh_status.get("source_monitoring", {})),
        }
        _refresh_status["quality"] = quality_report
        _write_json(QUALITY_REPORT_FILE, quality_report)

        _write_json(ARTICLES_FILE, merged)

        elapsed = time.monotonic() - start_ts
        log.info(
            "collect_articles done in %.1fs — %d new, %d total",
            elapsed,
            len(new_unique),
            len(merged),
        )
        _refresh_status["last_run"] = datetime.now(timezone.utc).isoformat()
        _refresh_status["initial_done"] = True

    except Exception as exc:
        log.exception("collect_articles failed: %s", exc)
        _refresh_status["last_error"] = str(exc)
        _refresh_status["initial_done"] = True  # allow frontend to show error state

    finally:
        _refresh_status["running"] = False


# ---------------------------------------------------------------------------
# FastAPI startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    """On startup: kick off initial collection + schedule recurring jobs."""
    # Schedule article collection every 30 minutes
    scheduler.add_job(
        collect_articles,
        CronTrigger(minute="*/30", timezone="Asia/Jerusalem"),
        id="daily_collect",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # Schedule weekly discovery every Sunday at 04:00
    scheduler.add_job(
        discover_sources,
        CronTrigger(day_of_week="sun", hour=4, minute=0, timezone="Asia/Jerusalem"),
        id="weekly_discover",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler started (daily collect + weekly discovery)")

    # Fire initial collection in background (don't block startup)
    asyncio.create_task(_initial_collection())


async def _initial_collection():
    """Run discovery then collection on first launch."""
    await asyncio.sleep(1)  # Let server start accepting requests first
    log.info("Initial startup: running source discovery …")
    await discover_sources()
    log.info("Initial startup: running article collection …")
    await collect_articles()


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    # Served as plain HTML (no Jinja syntax used) to sidestep Jinja/Starlette
    # cache-key incompatibility under Python 3.14.
    html_path = BASE_DIR / "templates" / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/api/articles")
async def get_articles(
    period: str = Query("today", regex="^(today|last24h|yesterday|week|month|all)$"),
    source: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    """
    Return articles filtered by period and optionally by source_id.
    """
    articles = [_sanitize_article(a) for a in _load_articles() if _is_supported_article(a)]

    now = datetime.now(IL_TZ)
    start_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_yesterday = start_of_today - timedelta(days=1)
    cutoffs = {
        "today": start_of_today,
        "last24h": now - timedelta(hours=24),
        "week": now - timedelta(days=7),
        "month": now - timedelta(days=30),
    }
    cutoff = cutoffs.get(period)

    filtered = []
    for a in articles:
        pub = _parse_date(a.get("published_at", ""))
        pub_local = pub.astimezone(IL_TZ) if pub else None
        if period == "yesterday":
            if not pub_local or not (start_of_yesterday <= pub_local < start_of_today):
                continue
        elif cutoff:
            if not pub_local or pub_local < cutoff:
                continue
        if source and a.get("source_id") != source and a.get("source") != source:
            continue
        filtered.append(a)

    def _display_score(a: dict) -> int:
        score = 0
        u = (a.get("url") or "").lower()
        if u and "news.google.com" not in u:
            score += 4
        if (a.get("image") or "") and not _is_weak_image_url(a.get("image", "")):
            score += 2
        if len((a.get("description") or "").strip()) >= 60:
            score += 1
        return score

    # Final response-level dedupe: protects UI from legacy duplicates where
    # the same story appears once as publisher URL and once as GNews redirect.
    deduped: list[dict] = []
    idx_by_canonical: dict[str, int] = {}
    idx_by_fallback: dict[str, int] = {}
    idx_by_day: dict[str, int] = {}
    idx_by_timestamp: dict[str, int] = {}
    for a in filtered:
        canonical = _canonical_article_url(a)
        if not canonical:
            canonical = _normalize_url_for_dedupe(a.get("url", ""))
        t = _clean_text(a.get("title", "")).strip().lower()
        s = (a.get("source_id") or a.get("source") or "").strip().lower()
        p = (a.get("published_at") or "").strip()
        fallback = f"{t}||{s}||{p}" if t and s and p else ""
        day = p[:10] if len(p) >= 10 else ""
        day_key = f"{t}||{s}||{day}" if t and s and day else ""
        first_word = t.split()[0] if t.split() else ""
        ts_key = (
            f"{first_word}||{s}||{p}"
            if first_word and len(first_word) >= 3 and s and p
            else ""
        )
        prev_idx = None
        if canonical:
            prev_idx = idx_by_canonical.get(canonical)
        if prev_idx is None and fallback:
            prev_idx = idx_by_fallback.get(fallback)
        if prev_idx is None and day_key:
            prev_idx = idx_by_day.get(day_key)
        if prev_idx is None and ts_key:
            prev_idx = idx_by_timestamp.get(ts_key)

        if prev_idx is None and not canonical and not fallback and not day_key and not ts_key:
            deduped.append(a)
            continue

        if prev_idx is None:
            idx = len(deduped)
            deduped.append(a)
            if canonical:
                idx_by_canonical[canonical] = idx
            if fallback:
                idx_by_fallback[fallback] = idx
            if day_key:
                idx_by_day[day_key] = idx
            if ts_key:
                idx_by_timestamp[ts_key] = idx
            continue
        if _display_score(a) > _display_score(deduped[prev_idx]):
            deduped[prev_idx] = a
            if canonical:
                idx_by_canonical[canonical] = prev_idx
            if fallback:
                idx_by_fallback[fallback] = prev_idx
            if day_key:
                idx_by_day[day_key] = prev_idx
            if ts_key:
                idx_by_timestamp[ts_key] = prev_idx

    return JSONResponse(
        content={
            "articles": deduped[:limit],
            "total": len(deduped),
            "period": period,
            "source_filter": source,
            "refresh_status": _refresh_status,
        }
    )


@app.post("/api/refresh")
async def manual_refresh():
    """Trigger a manual article collection run."""
    if _refresh_status["running"]:
        return JSONResponse(
            content={"status": "already_running", "message": "איסוף נמצא כבר בריצה"},
            status_code=202,
        )
    asyncio.create_task(collect_articles())
    return JSONResponse(content={"status": "started", "message": "איסוף מידע החל"})


@app.get("/api/sources")
async def get_sources():
    """Return all known sources."""
    sources = _load_sources()
    articles = [_sanitize_article(a) for a in _load_articles()]
    used_source_ids = {
        a.get("source_id") or a.get("source")
        for a in articles
        if _is_supported_article(a)
    }
    filtered_sources = [
        s for s in sources
        if s.get("id") in used_source_ids or s.get("name") in used_source_ids
    ]
    deduped_sources = []
    seen_labels = set()
    for src in filtered_sources:
        label = (src.get("display_name") or src.get("name") or "").strip().lower()
        if not label or label in seen_labels:
            continue
        seen_labels.add(label)
        deduped_sources.append(src)
    return JSONResponse(content={"sources": deduped_sources, "total": len(deduped_sources)})


@app.get("/api/stats")
async def get_stats():
    """
    Return aggregated stats:
    - total_today, total_week, total_month, total_all
    - week_over_week trend (this week vs last week)
    - top_sources (this week)
    - articles_by_day (last 30 days)
    - refresh_status
    """
    articles = [_sanitize_article(a) for a in _load_articles() if _is_supported_article(a)]
    now = datetime.now(IL_TZ)
    start_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    def count_since(days: int) -> int:
        cutoff = now - timedelta(days=days)
        return sum(
            1 for a in articles
            if (dt := _parse_date(a.get("published_at", "")))
            and dt.astimezone(IL_TZ) >= cutoff
        )

    total_today = sum(
        1 for a in articles
        if (dt := _parse_date(a.get("published_at", "")))
        and dt.astimezone(IL_TZ) >= start_of_today
    )
    total_week = count_since(7)
    total_last_week = sum(
        1 for a in articles
        if (dt := _parse_date(a.get("published_at", "")))
        and (now - timedelta(days=14)) <= dt.astimezone(IL_TZ) < (now - timedelta(days=7))
    )
    total_month = count_since(30)
    total_all = len(articles)

    # Week-over-week trend
    wow_delta = total_week - total_last_week
    wow_pct = (
        round(wow_delta / total_last_week * 100, 1)
        if total_last_week > 0
        else (100.0 if total_week > 0 else 0.0)
    )

    # Top sources this week
    week_cutoff = now - timedelta(days=7)
    source_counts: dict[str, int] = {}
    for a in articles:
        dt = _parse_date(a.get("published_at", ""))
        if dt and dt.astimezone(IL_TZ) >= week_cutoff:
            sid = a.get("source_id") or a.get("source") or "unknown"
            source_counts[sid] = source_counts.get(sid, 0) + 1

    top_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)[:8]

    # Articles by day (last 30 days)
    by_day: dict[str, int] = {}
    month_cutoff = now - timedelta(days=30)
    for a in articles:
        dt = _parse_date(a.get("published_at", ""))
        if dt and dt.astimezone(IL_TZ) >= month_cutoff:
            day = dt.astimezone(IL_TZ).strftime("%Y-%m-%d")
            by_day[day] = by_day.get(day, 0) + 1

    return JSONResponse(
        content={
            "total_today": total_today,
            "total_week": total_week,
            "total_last_week": total_last_week,
            "total_month": total_month,
            "total_all": total_all,
            "wow_delta": wow_delta,
            "wow_pct": wow_pct,
            "top_sources": [{"source": s, "count": c} for s, c in top_sources],
            "articles_by_day": [{"date": d, "count": c} for d, c in sorted(by_day.items())],
            "refresh_status": _refresh_status,
        }
    )


@app.get("/api/quality")
async def get_quality():
    """Return the latest quality report (written after each collect run)."""
    if QUALITY_REPORT_FILE.exists():
        try:
            data = json.loads(QUALITY_REPORT_FILE.read_text(encoding="utf-8"))
            return JSONResponse(content=data)
        except Exception as exc:
            return JSONResponse(content={"error": str(exc)}, status_code=500)
    # Fall back to in-memory (populated only if refresh ran this process lifecycle)
    live = _refresh_status.get("quality", {})
    if live:
        return JSONResponse(content=live)
    return JSONResponse(content={"error": "quality_report.json not yet available — run a refresh first"}, status_code=404)


@app.get("/api/status")
async def get_status():
    """Lightweight status endpoint for frontend polling."""
    articles = [_sanitize_article(a) for a in _load_articles() if _is_supported_article(a)]
    return JSONResponse(
        content={
            "running": _refresh_status["running"],
            "initial_done": _refresh_status["initial_done"],
            "last_run": _refresh_status["last_run"],
            "last_error": _refresh_status["last_error"],
            "coverage": _refresh_status.get("coverage", {}),
            "source_monitoring": _refresh_status.get("source_monitoring", {}),
            "quality": _refresh_status.get("quality", {}),
            "article_count": len(articles),
        }
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host=host, port=port, reload=False)
