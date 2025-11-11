thonimport argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from extractors.ad_parser import parse_advertiser_ads
from outputs.data_exporter import DataExporter

LOGGER = logging.getLogger("google_ad_transparency_scraper")

def configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

def load_settings(settings_path: Path) -> Dict[str, Any]:
    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file not found: {settings_path}")

    with settings_path.open("r", encoding="utf-8") as f:
        settings = json.load(f)

    # Basic sanity defaults
    settings.setdefault("mode", "offline")
    settings.setdefault("maxPages", 0)
    settings.setdefault("concurrency", 4)
    settings.setdefault("output", {}).setdefault("path", "data/output.json")
    settings.setdefault("http", {}).setdefault("timeout", 15)
    return settings

def load_advertiser_definitions(advertisers_path: Path) -> List[Dict[str, Any]]:
    if not advertisers_path.exists():
        raise FileNotFoundError(f"Advertisers file not found: {advertisers_path}")

    with advertisers_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    advertisers = data.get("advertisers")
    if not isinstance(advertisers, list):
        raise ValueError("Invalid advertisers.sample.json format: 'advertisers' must be a list")

    valid_advertisers = []
    for item in advertisers:
        if not isinstance(item, dict):
            continue
        adv_id = item.get("advertiserId")
        if not adv_id:
            LOGGER.warning("Skipping advertiser entry without advertiserId: %r", item)
            continue
        valid_advertisers.append(item)

    return valid_advertisers

def fetch_ads_offline_for_advertiser(
    advertiser: Dict[str, Any],
    max_pages: int,
) -> List[Dict[str, Any]]:
    """
    Offline mode: use bundled sample data from advertisers.sample.json.
    """
    raw_ads = advertiser.get("ads") or []
    if not isinstance(raw_ads, list):
        LOGGER.warning(
            "Advertiser %s has invalid 'ads' field; expected list, got %r",
            advertiser.get("advertiserId"),
            type(raw_ads),
        )
        return []

    # Simulate paging by slicing list
    if max_pages and max_pages > 0:
        page_size = 40  # realistically, Google Ads Transparency shows 40 ads/page
        max_ads = page_size * max_pages
        raw_ads = raw_ads[:max_ads]

    LOGGER.debug(
        "Loaded %d offline ads for advertiser %s",
        len(raw_ads),
        advertiser.get("advertiserId"),
    )
    return raw_ads

def fetch_ads_for_advertiser(
    advertiser: Dict[str, Any],
    settings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Adapter for different data sources. Right now, it primarily uses offline
    sample data, but the structure is ready for live HTTP scraping if needed.
    """
    mode = settings.get("mode", "offline").lower()
    max_pages = int(settings.get("maxPages", 0) or 0)

    if mode == "offline":
        return fetch_ads_offline_for_advertiser(advertiser, max_pages=max_pages)

    # Placeholder for potential online mode. Since we don't rely on any private
    # or unstable endpoint here, we'll simply log and fall back to offline data.
    LOGGER.warning(
        "Online mode requested but not implemented in this bundle. "
        "Falling back to offline sample data for advertiser %s.",
        advertiser.get("advertiserId"),
    )
    return fetch_ads_offline_for_advertiser(advertiser, max_pages=max_pages)

def process_advertiser(
    advertiser: Dict[str, Any],
    settings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    advertiser_id = advertiser.get("advertiserId")
    advertiser_name = advertiser.get("advertiserName", "Unknown Advertiser")

    LOGGER.info("Processing advertiser %s (%s)", advertiser_name, advertiser_id)

    try:
        raw_ads = fetch_ads_for_advertiser(advertiser, settings=settings)
        normalized_ads = parse_advertiser_ads(
            advertiser_id=advertiser_id,
            advertiser_name=advertiser_name,
            raw_ads=raw_ads,
        )
        LOGGER.info(
            "Parsed %d ads for advertiser %s (%s)",
            len(normalized_ads),
            advertiser_name,
            advertiser_id,
        )
        return normalized_ads
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception(
            "Error processing advertiser %s (%s): %s",
            advertiser_name,
            advertiser_id,
            exc,
        )
        return []

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Google Ads Transparency Scraper (offline sample implementation)."
    )
    parser.add_argument(
        "--settings",
        type=str,
        default="src/config/settings.example.json",
        help="Path to JSON settings file.",
    )
    parser.add_argument(
        "--advertisers",
        type=str,
        default="data/advertisers.sample.json",
        help="Path to advertisers sample JSON file.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON file path (overrides settings.output.path if provided).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional max pages override (0 means unlimited).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase log verbosity (use -vv for debug).",
    )
    return parser.parse_args(argv)

def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    settings_path = Path(args.settings)
    advertisers_path = Path(args.advertisers)

    try:
        settings = load_settings(settings_path)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load settings from %s: %s", settings_path, exc)
        return 1

    if args.max_pages is not None:
        settings["maxPages"] = args.max_pages

    try:
        advertisers = load_advertiser_definitions(advertisers_path)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load advertisers from %s: %s", advertisers_path, exc)
        return 1

    if not advertisers:
        LOGGER.error("No valid advertisers found in %s", advertisers_path)
        return 1

    all_ads: List[Dict[str, Any]] = []

    concurrency = int(settings.get("concurrency", 4) or 1)
    LOGGER.info("Starting scrape for %d advertisers (concurrency=%d)", len(advertisers), concurrency)

    if concurrency <= 1 or len(advertisers) == 1:
        # Simple sequential processing
        for advertiser in advertisers:
            all_ads.extend(process_advertiser(advertiser, settings=settings))
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(process_advertiser, advertiser, settings): advertiser
                for advertiser in advertisers
            }
            for future in as_completed(futures):
                adv = futures[future]
                try:
                    all_ads.extend(future.result())
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception(
                        "Unhandled exception processing advertiser %s (%s): %s",
                        adv.get("advertiserName", "Unknown"),
                        adv.get("advertiserId"),
                        exc,
                    )

    LOGGER.info("Total normalized ads collected: %d", len(all_ads))

    output_path_str = args.output or settings.get("output", {}).get("path", "data/output.json")
    exporter = DataExporter(output_path=Path(output_path_str))

    try:
        final_path = exporter.export(all_ads)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to export data: %s", exc)
        return 1

    LOGGER.info("Scrape completed successfully. Output written to %s", final_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))