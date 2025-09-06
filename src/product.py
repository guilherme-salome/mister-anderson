#!/usr/bin/env python3
from dataclasses import dataclass, field
from typing import List, Optional
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

    @property
    def asset_tag(self) -> str:
        to_hash = f"{self.created_at}-{self.created_by}"
        digest = hashlib.sha1(to_hash.encode()).hexdigest()
        logger.info(f"Asset tag is {digest}")
        return digest

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
