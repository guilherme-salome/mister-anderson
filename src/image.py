#!/usr/bin/env python3

import os
import time
import logging
import asyncio
from telegram import Update
from telegram.ext import ContextTypes


from .menu import get_menu


logger = logging.getLogger(__name__)


IMAGE_MIME_TYPES = {
    "image/jpeg", "image/png", "image/gif", "image/bmp", "image/webp", "image/tiff"
}


async def image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    product = context.chat_data.get("product")
    logger.info(f"Current Product: {product}")
    if not product:
        return

    file_path = None

    if update.message.photo:
        photo = update.message.photo[-1] # highest resolution available
        file = await context.bot.get_file(photo.file_id)
        file_path = os.path.join(product["path"], f"{photo.file_id}.jpg")

    if update.message.document:
        doc = update.message.document
        if doc.mime_type in IMAGE_MIME_TYPES:
            file = await context.bot.get_file(doc.file_id)
            ext = doc.file_name.split(".")[-1] if doc.file_name else "img"
            file_path = os.path.join(product["path"], f"{doc.file_id}.{ext}")

    if file_path:
        await file.download_to_drive(file_path)
        context.chat_data.get("product")["photos"].append(file_path)
        markup = get_menu(pickup=product["pickup"], product=product)
        await update.message.reply_text("Image received and saved!", reply_markup=markup)
