#!/usr/bin/env python3
import os
import logging
import time


from telegram import Update
from telegram.ext import ContextTypes


SAVE_DIR = "data"

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


def start_new_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    product_id = str(get_next_product_number())
    product =  {
        "pickup": context.chat_data.get("pickup"),
        "created_at": time.time(),
        "created_by": update.effective_user.id,
        "id": product_id,
        "chat_id": update.effective_chat.id,
        "path": os.path.join(SAVE_DIR, product_id),
        "photos": []
    }
    os.makedirs(product["path"], exist_ok = True)
    logger.info(f"New product group created: {product}")
    return product
