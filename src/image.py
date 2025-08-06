#!/usr/bin/env python3

import os
import time
import logging
from telegram import Update
from telegram.ext import ContextTypes

from .product import start_new_product


logger = logging.getLogger(__name__)

GROUP_TIMEOUT = 600  # seconds (10 min) for auto new group
IMAGE_MIME_TYPES = {
    "image/jpeg", "image/png", "image/gif", "image/bmp", "image/webp", "image/tiff"
}


async def image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Create a new product if necessary
    user_id = update.effective_user.id
    product = context.chat_data.get("product")
    logger.info(f"Current Product: {product}")
    if (not product or time.time() - product['created_at'] > GROUP_TIMEOUT):
        product = start_new_product(user_id)
        context.chat_data["product"] = product

    # Save images to product folder
    if update.message.photo:
        photo = update.message.photo[-1] # highest resolution available
        file = await context.bot.get_file(photo.file_id)
        file_path = os.path.join(product["path"], f"{photo.file_id}.jpg")
        await file.download_to_drive(file_path)
        await update.message.reply_text("Image received and saved!")

    if update.message.document:
        doc = update.message.document
        if doc.mime_type in IMAGE_MIME_TYPES:
            file = await context.bot.get_file(doc.file_id)
            ext = doc.file_name.split(".")[-1] if doc.file_name else "img"
            file_path = os.path.join(product["path"], f"{doc.file_id}.{ext}")
            await file.download_to_drive(file_path)
            await update.message.reply_text("Image document received and saved!")
        else:
            await update.message.reply_text("Document is not an image.")
