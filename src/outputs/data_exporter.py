thonfrom __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterable, List

LOGGER = logging.getLogger("google_ad_transparency_scraper.data_exporter")

class DataExporter:
    """
    Responsible for exporting normalized ad data into a JSON file on disk.
    """

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path

    def export(self, records: Iterable[dict[str, Any]]) -> Path:
        """
        Write all records to the configured JSON file.

        The file is created with pretty-printing and UTF-8 encoding. Any
        non-serializable values are converted into strings.
        """
        records_list: List[dict[str, Any]] = list(records)
        output_dir = self.output_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        tmp_path = self.output_path.with_suffix(self.output_path.suffix + ".tmp")

        LOGGER.debug(
            "Writing %d records to temporary file %s", len(records_list), tmp_path
        )

        def default_serializer(obj: Any) -> Any:
            try:
                return str(obj)
            except Exception:  # noqa: BLE001
                return "UNSERIALIZABLE"

        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(
                records_list,
                f,
                ensure_ascii=False,
                indent=2,
                default=default_serializer,
            )

        # Replace any previous file atomically
        tmp_path.replace(self.output_path)

        LOGGER.info("Exported %d records to %s", len(records_list), self.output_path)
        return self.output_path