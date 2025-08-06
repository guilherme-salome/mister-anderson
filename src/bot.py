#!/usr/bin/env python3

import logging
import os
import time
from telegram import Update
from telegram.ext import filters, MessageHandler, ApplicationBuilder, CommandHandler, ContextTypes

from .config import read_token
from .image import image
from .text import text


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def main():
    application = ApplicationBuilder().token(read_token('api.telegram.com')).build()
    text_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), text)
    image_handler = MessageHandler(filters.PHOTO | filters.Document.IMAGE, image)

    application.add_handler(text_handler)
    application.add_handler(image_handler)
    application.run_polling()


if __name__ == '__main__':
    main()
