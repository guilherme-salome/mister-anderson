#!/usr/bin/env python3

import logging
import os
import time
from telegram import Update
from telegram.ext import filters, MessageHandler, ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

from .config import read_token
from .image import image
from .menu import start, _test_product, on_callback, on_reply

logging.basicConfig(
    format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def main():
    application = ApplicationBuilder().token(read_token('api.telegram.com')).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("test_product", _test_product))
    application.add_handler(CallbackQueryHandler(on_callback, pattern="^act:"))
    application.add_handler(MessageHandler(filters.TEXT, on_reply))
    application.add_handler(MessageHandler(filters.PHOTO | filters.Document.IMAGE, image))
    application.run_polling()


if __name__ == '__main__':
    main()
