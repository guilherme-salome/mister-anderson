#!/usr/bin/env python3
import os
import logging
import time


SAVE_DIR = "products"

logger = logging.getLogger(__name__)


def get_next_product_number():
    """
    Scans the SAVE_DIR for numeric folder names and
    returns the next unused product number (int).
    """
    ids = set([0])
    os.makedirs(SAVE_DIR, exist_ok = True)
    for name in os.listdir(SAVE_DIR):
        if name.isdigit():
            ids.add(int(name))
    return max(ids) + 1


def start_new_product(user_id):
    product =  {
        "created_at": time.time(),
        "created_by": user_id,
        "id": str(get_next_product_number()),
    }
    product["path"] = os.path.join(SAVE_DIR, product["id"])
    os.makedirs(product["path"], exist_ok = True)
    logger.info(f"New product group created: {product}")
    return product
