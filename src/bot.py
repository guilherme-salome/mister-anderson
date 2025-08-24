#!/usr/bin/env python3

import logging
import os
import time
from telegram import Update
from telegram.ext import filters, MessageHandler, ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

from .config import read_token
from .image import image
from .text import text
from .menu import start, button, pickup_reply

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def main():
    application = ApplicationBuilder().token(read_token('api.telegram.com')).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT, pickup_reply))
    application.add_handler(MessageHandler(filters.PHOTO | filters.Document.IMAGE, image))
    application.run_polling()


if __name__ == '__main__':
    main()
