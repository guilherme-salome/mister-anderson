#!/usr/bin/env python3

import logging
import os
import re
import time
from telegram import Update
from telegram.ext import filters, MessageHandler, ApplicationBuilder, CommandHandler, ContextTypes


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


GROUP_TIMEOUT = 600  # seconds (10 min) for auto new group
IMAGE_SAVE_DIR = "images"
IMAGE_MIME_TYPES = {
    "image/jpeg", "image/png", "image/gif", "image/bmp", "image/webp", "image/tiff"
}


def read_telegram_token():
    with open(os.path.expanduser('~/.authinfo')) as f:
        for line in f:
            if 'machine api.telegram.com' in line:
                m = re.search(r'password\s+(\S+)', line)
                if m:
                    return m.group(1)
    raise ValueError("Telegram token not found in ~/.authinfo")


user_groups = {}  # user_id -> current group info


def start_new_group(user_id, cur_time=None):
    """Initialize a new group for user"""
    group = {
        "start_time": cur_time if cur_time is not None else time.time(),
        "images": []
    }
    user_groups[user_id] = group


async def text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    if text.lower().startswith("new"):
        start_new_group(user_id, time.time())
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Started a new product group! (user: {user_id})")
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=update.message.text)


async def image(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not os.path.exists(IMAGE_SAVE_DIR):
        os.makedirs(IMAGE_SAVE_DIR)

    user_id = update.effective_user.id
    if (user_id not in user_groups or
        time.time() - user_groups[user_id]['start_time'] > GROUP_TIMEOUT):
        start_new_group(user_id)

    if update.message.photo:
        # Get the highest-resolution photo
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        file_path = os.path.join(
            IMAGE_SAVE_DIR,
            f"{photo.file_id}.jpg"
        )
        await file.download_to_drive(file_path)
        # Respond to the user
        await update.message.reply_text("Image received and saved!")

    if update.message.document:
        doc = update.message.document
        if doc.mime_type in IMAGE_MIME_TYPES:
            file = await context.bot.get_file(doc.file_id)
            ext = doc.file_name.split(".")[-1] if doc.file_name else "img"
            file_path = os.path.join(IMAGE_SAVE_DIR, f"{doc.file_id}.{ext}")
            await file.download_to_drive(file_path)
            await update.message.reply_text("Image document received and saved!")
        else:
            await update.message.reply_text("Document is not an image.")


def main():
    application = ApplicationBuilder().token(read_telegram_token()).build()
    text_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), text)
    image_handler = MessageHandler(filters.PHOTO | filters.Document.IMAGE, image)

    application.add_handler(text_handler)
    application.add_handler(image_handler)
    application.run_polling()


if __name__ == '__main__':
    main()
