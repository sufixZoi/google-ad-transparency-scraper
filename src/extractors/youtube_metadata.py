thonfrom __future__ import annotations

import logging
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

LOGGER = logging.getLogger("google_ad_transparency_scraper.youtube_metadata")

def _extract_video_id_from_url(url: str) -> str | None:
    """
    Extract a YouTube video ID from a variety of URL formats.

    Supported examples:
      - https://www.youtube.com/watch?v=VIDEO_ID
      - https://youtu.be/VIDEO_ID
    """
    if not url:
        return None

    try:
        parsed = urlparse(url)
    except Exception:  # noqa: BLE001
        return None

    if "youtube.com" in parsed.netloc:
        qs = parse_qs(parsed.query or "")
        vid = qs.get("v", [None])[0]
        if vid:
            return vid

        # Fallback: sometimes the ID appears at the end of the path
        segments = (parsed.path or "").strip("/").split("/")
        if segments:
            return segments[-1] or None

    if "youtu.be" in parsed.netloc:
        # Path is typically /VIDEO_ID
        segments = (parsed.path or "").strip("/").split("/")
        if segments:
            return segments[0] or None

    return None

def ensure_youtube_metadata(variation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure that the given variation dict contains a 'youtubeMetadata' field with:
      - adId
      - youtubeUrl
      - ctaUrl (optional)
    """
    variation = dict(variation)  # shallow copy to avoid mutating original

    existing_meta = variation.get("youtubeMetadata")
    if isinstance(existing_meta, dict):
        # Make sure required fields exist
        youtube_url = existing_meta.get("youtubeUrl") or existing_meta.get("url") or ""
        cta_url = existing_meta.get("ctaUrl") or existing_meta.get("cta") or ""
        ad_id = existing_meta.get("adId") or _extract_video_id_from_url(youtube_url) or ""

        variation["youtubeMetadata"] = {
            "adId": ad_id,
            "youtubeUrl": youtube_url,
            "ctaUrl": cta_url,
        }
        return variation

    # No youtubeMetadata present – look for URL hints at the top level
    youtube_url = (
        variation.get("youtubeUrl")
        or variation.get("videoUrl")
        or variation.get("url")
        or ""
    )
    cta_url = variation.get("ctaUrl") or variation.get("cta") or ""

    ad_id = _extract_video_id_from_url(youtube_url) or ""

    variation["youtubeMetadata"] = {
        "adId": ad_id,
        "youtubeUrl": youtube_url,
        "ctaUrl": cta_url,
    }
    return variation