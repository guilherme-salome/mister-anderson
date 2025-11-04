#!/usr/bin/env python3
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import time
import datetime
import os
import tempfile
import hashlib
import shutil
import logging
import atexit


logger = logging.getLogger(__name__)


@dataclass
class Product:
    created_by: str
    tempdir: str = field(default_factory=tempfile.mkdtemp)
    created_at: str = field(default_factory=lambda: str(datetime.datetime.today()))
    photos: List[str] = field(default_factory=list)
    quantity: int = field(default_factory=lambda: 1)
    pickup: Optional[str] = None
    description_raw: str = ""
    description_json: Dict[str, Any] = field(default_factory=dict)

    @property
    def asset_tag(self) -> str:
        to_hash = f"{self.created_at}-{self.created_by}"
        digest = hashlib.sha1(to_hash.encode()).hexdigest()[:10]
        logger.info(f"Asset tag is {digest}")
        return digest

    @property
    def serial_number(self) -> str:
        return self.description_json.get("serial_number", "")

    @property
    def short_description(self) -> str:
        return self.description_json.get("short_description", "")

    @property
    def destination(self) -> str:
        return (
            self.description_json.get("destination_label")
            or self.description_json.get("destination", "")
        )

    @property
    def subcategory(self) -> str:
        value = self.description_json.get("subcategory")
        if value:
            return value
        legacy = self.description_json.pop("commodity", None)
        if legacy:
            self.description_json["subcategory"] = legacy
            return legacy
        return ""

    @property
    def cod_destiny(self) -> Optional[int]:
        value = self.description_json.get("cod_destiny")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @property
    def destination_reason(self) -> str:
        return (
            self.description_json.get("destination_reason")
            or self.description_json.get("reason", "")
        )

    @property
    def grade(self) -> Optional[int]:
        value = self.description_json.get("grade")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def __post_init__(self):
        logger.info(f"New Instance of {repr(self)}")
        atexit.register(self.clean_tempdir)

    def clean_tempdir(self):
        logger.info(f"Cleaning up {self.tempdir}")
        shutil.rmtree(self.tempdir, ignore_errors = True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(filename)s %(funcName)s %(levelname)s: %(message)s"
    )
    product = Product("testing-user")
    product.asset_tag
