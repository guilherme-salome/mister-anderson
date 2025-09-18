#!/usr/bin/env python3

import logging
import os
import time
from telegram import Update
from telegram.ext import filters, MessageHandler, ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

from .config import read_token, ALLOWED_USERS
from .image import image
from .menu import start, _test_product, on_callback, on_reply

logging.basicConfig(
    format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

allowed_filter = filters.User(user_id=ALLOWED_USERS) if ALLOWED_USERS else None
logger.info(f"Whitelisted Users: {allowed_filter}")


def main():
    application = ApplicationBuilder().token(read_token('api.telegram.com')).build()
    application.add_handler(CommandHandler("start", start, filters=allowed_filter))
    application.add_handler(CommandHandler("test_product", _test_product, filters=allowed_filter))
    application.add_handler(MessageHandler(filters.TEXT & allowed_filter, on_reply))
    application.add_handler(MessageHandler((filters.PHOTO | filters.Document.IMAGE) & allowed_filter, image))
    application.add_handler(CallbackQueryHandler(on_callback, pattern="^act:"))
    application.run_polling()


if __name__ == '__main__':
    main()
