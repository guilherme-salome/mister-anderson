#!/usr/bin/env python3

import asyncio
from telegram import Update
from telegram.ext import ContextTypes

from .product import start_new_product
from .llm import process_product_folder


async def text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.strip().lower().startswith("new"):
        previous = context.chat_data.get("product")
        if previous:
            asyncio.create_task(process_product_folder(previous, context))
        context.chat_data["product"] = start_new_product(update.effective_user.id, update.effective_chat.id)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Started a new product group!")
