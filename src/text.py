#!/usr/bin/env python3

from telegram import Update
from telegram.ext import ContextTypes

from .product import start_new_product


async def text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.strip().lower().startswith("new"):
        context.chat_data["product"] = start_new_product(update.effective_user.id, update.effective_chat.id)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Started a new product group!")
