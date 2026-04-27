"""
Google News URL Resolver
========================
Converts Google News RSS redirect URLs (https://news.google.com/rss/articles/CBMia...)
to real article URLs using a tiered approach:

1. Fast path: Local URL-safe base64 decode
2. Primary: Batchexecute API (extract tokens + POST to /DotsSplashUi/data/batchexecute)
3. Fallback: Return source_url as-is if all else fails

Usage:
  resolver = GoogleNewsURLResolver(cache_file="gnews_url_cache.json")
  real_url = resolver.resolve("https://news.google.com/rss/articles/CBMia...")
"""

import base64
import json
import logging
import re
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime, timedelta
from googlenewsdecoder import new_decoderv1

logger = logging.getLogger(__name__)

class GoogleNewsURLResolver:
    """Tiered resolver for Google News redirect URLs to real article URLs."""

    def __init__(self, cache_file: str = "gnews_url_cache.json", cache_ttl_hours: int = 7 * 24):
        """
        Args:
            cache_file: Path to JSON cache file storing CBM ID → real URL mappings
            cache_ttl_hours: Cache validity in hours (default: 7 days)
        """
        self.cache_file = Path(cache_file)
        self.cache_ttl_hours = cache_ttl_hours
        self.cache = self._load_cache()
        
    def _load_cache(self) -> Dict[str, dict]:
        """Load cache from file; return empty dict if not found."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f) or {}
            except Exception as e:
                logger.warning(f"Failed to load resolver cache: {e}")
                return {}
        return {}

    def _save_cache(self):
        """Persist cache to file."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Failed to save resolver cache: {e}")

    def _extract_cbm_id(self, url: str) -> Optional[str]:
        """Extract CBM ID from Google News URL.
        
        E.g., https://news.google.com/rss/articles/CBMia... → CBMia...
        """
        match = re.search(r'/articles/(CBM[^/?]+)', url)
        if match:
            return match.group(1)
        return None

    def _is_cache_valid(self, entry: dict) -> bool:
        """Check if cache entry is not expired."""
        if 'cached_at' not in entry:
            return False
        cached_at = datetime.fromisoformat(entry['cached_at'])
        ttl = timedelta(hours=self.cache_ttl_hours)
        return datetime.now() - cached_at < ttl

    def _tier_1_base64_decode(self, cbm_id: str) -> Optional[str]:
        """Tier 1: Try simple base64 decode.
        
        Returns URL if successful, None otherwise.
        """
        try:
            # URL-safe base64 decode
            padding = (4 - len(cbm_id) % 4) % 4
            padded = cbm_id + '=' * padding
            decoded = base64.urlsafe_b64decode(padded)
            
            # Try to extract as UTF-8 string
            text = decoded.decode('utf-8', errors='ignore')
            
            # Heuristic: if starts with http and contains no null bytes, likely valid
            if text.strip().startswith('http') and '\x00' not in text:
                # Extract URL (may have trailing junk)
                match = re.search(r'https?://[^\x00]+', text)
                if match:
                    url = match.group(0).strip()
                    # Basic sanity check
                    if 'news.google.com' not in url:
                        logger.debug(f"Tier 1 decode success: {cbm_id[:20]}... → {url[:80]}")
                        return url
        except Exception as e:
            pass
        
        return None

    def _tier_2_decoder_library(self, gnews_url: str) -> Optional[str]:
        """Tier 2: Use googlenewsdecoder library to resolve redirect URLs."""
        try:
            result = new_decoderv1(gnews_url)
            if isinstance(result, dict) and result.get("status"):
                resolved_url = (result.get("decoded_url") or "").strip()
                if resolved_url.startswith("http") and "news.google.com" not in resolved_url:
                    logger.debug("Tier 2 decoder success: %s", resolved_url[:120])
                    return resolved_url
            return None
        except Exception as exc:
            logger.debug("Tier 2 decoder exception: %s", exc)
            return None

    def resolve(self, gnews_url: str, source_url: Optional[str] = None) -> Optional[str]:
        """Resolve a Google News redirect URL to real article URL.
        
        Args:
            gnews_url: Google News redirect URL (https://news.google.com/rss/articles/CBMia...)
            source_url: Fallback source domain (e.g., https://gadgety.co.il)
            
        Returns:
            Real article URL or None if resolution failed.
        """
        cbm_id = self._extract_cbm_id(gnews_url)
        if not cbm_id:
            return None
        
        # Check cache
        if cbm_id in self.cache:
            entry = self.cache[cbm_id]
            if self._is_cache_valid(entry):
                logger.debug(f"Cache hit: {cbm_id[:20]}...")
                return entry.get('resolved_url')
            else:
                # Expire stale entry
                del self.cache[cbm_id]
        
        resolved_url = None
        
        # Tier 1: Fast base64 decode
        resolved_url = self._tier_1_base64_decode(cbm_id)
        if resolved_url:
            self._cache_result(cbm_id, resolved_url)
            return resolved_url
        
        # Tier 2: decoder library (internally handles Google's current format)
        resolved_url = self._tier_2_decoder_library(gnews_url)
        if resolved_url:
            self._cache_result(cbm_id, resolved_url)
            return resolved_url
        
        # Tier 3: Give up, return source_url as fallback
        if source_url:
            logger.debug(f"Resolver fallback to source_url for {cbm_id[:20]}...")
            return source_url
        
        logger.warning(f"Could not resolve Google News URL: {cbm_id}")
        return None

    def _cache_result(self, cbm_id: str, resolved_url: str):
        """Store resolution result in cache."""
        self.cache[cbm_id] = {
            'resolved_url': resolved_url,
            'cached_at': datetime.now().isoformat()
        }
        self._save_cache()

    def resolve_batch(self, gnews_urls: list, source_urls: Optional[list] = None) -> Dict[str, Optional[str]]:
        """Resolve multiple Google News URLs.
        
        Args:
            gnews_urls: List of Google News redirect URLs
            source_urls: Optional list of fallback source URLs (same order as gnews_urls)
            
        Returns:
            Dict mapping original URL → resolved URL (None if failed)
        """
        results = {}
        for i, url in enumerate(gnews_urls):
            source_url = source_urls[i] if source_urls and i < len(source_urls) else None
            results[url] = self.resolve(url, source_url)
        return results


# Example usage / testing
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    resolver = GoogleNewsURLResolver()
    
    # Test with a sample Google News URL
    test_url = "https://news.google.com/rss/articles/CBMiaEFVX3lxTE5lOFVRdEN4eFhRZkZ4dHp4MW51R0lYeXI3SjFDSUowZTJHY1ZXaEZPYTJldTlWX1N2c09tN1JvUktXdGppRzJaWXZhU0N0WE01QnZXMTUwbXY2U2tycG1IeE5OX1Roamtf"
    
    print(f"Original: {test_url[:80]}")
    resolved = resolver.resolve(test_url)
    print(f"Resolved: {resolved}")
