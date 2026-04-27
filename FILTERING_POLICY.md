# Filtering Policy — Intel Media Pulse

This document describes every rule that can cause an article to be **accepted or rejected** by the system.
Rules are evaluated in order inside `_support_rejection_reason()` in `app.py`.

---

## Evaluation Order

```
MANUAL_INCLUDE_URLS (allow-list)
  ↓ if not in list
legacy_archive_url
  ↓
non_article_listing_url
  ↓
hard_reject_title
  ↓
ITLC34 override (accept immediately)
  ↓
non_hebrew
  ↓
_meta_mentions_intel_significantly → accept
  ↓
body_intel == True → accept
  ↓
insufficient_intel_signal (reject)
```

---

## Rules Table

| Rule Code | Description | Code Location | Business Rationale |
|---|---|---|---|
| *(none — accepted)* | Article passes all gates | `_support_rejection_reason` returns `None` | Article is relevant Intel coverage in Hebrew |
| **`legacy_archive_url`** | URL matches Globes legacy `/Ald/ART/` docview pattern, indicating an old cached archive page | `_infer_legacy_date_from_url()` | Archive pages are duplicates of already-indexed content and cause stale articles to appear |
| **`non_article_listing_url`** | URL/source is a known non-editorial destination (Investing.com / statusinvest.com.br listing/instrument/news pages, Facebook profile page, Workday careers page, Calcalist topic page) | `_is_investing_non_article_url()`, `_is_non_editorial_url_or_source()` | Non-editorial pages and ticker/BDR listings are not journalistic coverage and create feed noise |
| **`hard_reject_title`** | Title matches a blocklist pattern — e.g. stock-instrument noise like "Intel חדשות מניית (INTC)" or "INTC \| מניית אינטל - Investing.com - שוק ההון..." | `_hard_reject_title()` → `_STOCK_INSTRUMENT_NEWS_RE`, `_INVESTING_INSTRUMENT_TITLE_RE`, `_INVESTING_PORTAL_SUFFIX_RE` | Financial ticker/listing headlines provide no editorial value |
| **`non_hebrew`** | Title + description contain no Hebrew characters, and no ITLC34 override applies | `_is_hebrew_article()` | System is scoped to Hebrew-language coverage; English/other articles are out of scope |
| **`insufficient_intel_signal`** | Article is Hebrew but neither title/description mention Intel significantly, nor was a body signal found | `_meta_mentions_intel_significantly()`, `body_intel` flag | Avoids irrelevant articles that mention Intel only tangentially or not at all |

---

## Special Overrides

| Override | Trigger | Effect | Code Location |
|---|---|---|---|
| **Manual allow-list** | URL is in `MANUAL_INCLUDE_URLS` set | Skip all rejection checks — article is always accepted | Top of `_support_rejection_reason()` |
| **ITLC34 pass-through** | ~~*(removed 2026-04-24)*~~ | Previously bypassed `non_hebrew` for ITLC34 BDR content. Removed along with Investing.com source — ITLC34 listings are Portuguese/English BDR noise, not editorial coverage. | — |
| **Body-intel flag** | `body_intel = True` set after HTML body extraction confirms "Intel" in body text | Overrides `insufficient_intel_signal` — article accepted | `body_intel` field in article, checked after metadata gates |

---

## Image Quality Rules

These rules affect which image is displayed but do **not** affect article acceptance/rejection.

| Rule | Description | Code Location |
|---|---|---|
| **Weak image** | Image URL matches publisher placeholder / UI-widget patterns (logo, icon, favicon, sprite, avatar, `noimage`, `placeholder`, `default-image`, `fallback`, Globes `/more.jpg`/`/less.png`/`/pass/close.png`, accessibility widgets `nagish`/`a11y`, Calcalist `tmmobile`/`tmdesktop`) or tiny URL-declared dimensions (w≤360 or h≤240) | `_is_weak_image_url()` + `_PLACEHOLDER_IMAGE_PATTERNS` |
| **Best image selection** | Scores all candidate image URLs, selects the one with highest resolution/relevance signals | `_pick_best_image_url()` |
| **OG-first enrichment** | Image enrichment always tries `og:image` first (publisher-controlled, reliable); Tavily is used for description and only as image fallback when OG is missing and Tavily's image passes `_is_weak_image_url` | `_enrich_article()` |
| **Weak cached image bypass** | Enrichment cache entries containing weak/placeholder images are ignored, so backfill re-fetches the real publisher image | `_enrich_article()` cache merge block |

---

## Drop Reason Codes (Quality Report)

The `/api/quality` endpoint returns counts for each drop reason:

```json
{
  "filtering": {
    "drop_reasons": {
      "legacy_archive_url": N,
      "non_article_listing_url": N,
      "hard_reject_title": N,
      "non_hebrew": N,
      "insufficient_intel_signal": N
    }
  }
}
```

---

## Modifying Policy

- To **add** a new hard-block title pattern: update `_STOCK_INSTRUMENT_NEWS_RE` or add to `_title_is_ticker_list()` in `app.py`
- To **always include** a specific URL: add it to `MANUAL_INCLUDE_URLS` in `app.py`
- To **adjust Intel signal threshold**: modify `_meta_mentions_intel_significantly()` scoring weights
- All changes to filtering logic must be reflected in this document and logged in `DECISIONS_LOG.md`
