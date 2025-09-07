#!/usr/bin/env python3

import logging
from typing import Optional

from enum import Enum
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.ext import ContextTypes


from .llm import process_product_folder
from .product import Product


logger = logging.getLogger(__name__)


class State(str, Enum):
    INIT = "INIT"                # no pickup set
    READY = "READY"              # pickup set, no product yet
    PRODUCT = "PRODUCT"          # gathering photos etc.
    ANALYZING = "ANALYZING"      # parsing photos and other information
    REVIEW = "REVIEW"            # user reviews and updates what is necessary
    DONE = "DONE"                # after confirmation


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = """
Hello! I’m Mr. Anderson, your assistant for processing deliveries.

I can help you quickly and accurately enter product details after a pickup by analyzing images and generating clear product descriptions.
Just send me a product photo to get started, and I’ll do the rest!

Please choose:
    """
    context.chat_data["state"] = State.INIT
    context.chat_data["product"] = Product(created_by = update.effective_user.id)
    markup = render(context.chat_data["state"], context.chat_data["product"])
    await update.message.reply_text(message, reply_markup=markup)


def render(state: str, product: Optional[Product] = None):
    logger.info(f"State: {state}")

    if state is not State.INIT and product is None:
        raise ValueError(f"product is required when state is {state}")

    if state == State.INIT:
        kb = [[InlineKeyboardButton("🚚 Set Pickup", callback_data="act:set:pickup")]]

    if state == State.READY:
        kb = [
            [InlineKeyboardButton(f"🚚 Pickup: {product.pickup}", callback_data="act:update:pickup")],
            [InlineKeyboardButton("📝 New Product", callback_data="act:set:product")],
        ]

    if state == State.PRODUCT:
        kb = [
            [InlineKeyboardButton(f"🚚 Pickup: {product.pickup}", callback_data="act:update:pickup")],
            [InlineKeyboardButton(f"📝 Product Tag: {product.asset_tag}", callback_data="noop:set:tag")],
            [InlineKeyboardButton(f"📦 Quantity: {product.quantity}", callback_data="act:update:quantity")],
            [InlineKeyboardButton(f"📷 Send Photo ({len(product.photos)})", callback_data="act:send:photo")],
            [InlineKeyboardButton("🤖 Analyze Product", callback_data="act:analyze")],
        ]

    if state == State.ANALYZING:
        pass

    if state == State.REVIEW:
        kb = [
            [InlineKeyboardButton(f"🚚 Pickup: {product.pickup}", callback_data="act:set:pickup")],
            [InlineKeyboardButton(f"📝 Product Tag: {product.asset_tag}", callback_data="noop:set:tag")],
            [InlineKeyboardButton(f"📦 Quantity: {product.quantity}", callback_data="act:set:quantity")],
            [InlineKeyboardButton("✅ Submit Product", callback_data="act:submit")],
        ]

    if state == State.DONE:
        kb = [
            [InlineKeyboardButton(f"🚚 Pickup: {product.pickup}", callback_data="act:set:pickup")],
            [InlineKeyboardButton("📝 New Product", callback_data="act:set:product")],
        ]

    return InlineKeyboardMarkup(kb)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    parts = data.split(":", 2)  # e.g., act:edit:serial_number
    if not parts or parts[0] != "act":
        return

    action = parts[1] if len(parts) > 1 else ""
    arg = parts[2] if len(parts) > 2 else None

    logger.info(f"Callback: {action} {arg}")

    if action == "noop":
        return

    if action == "set":

        if arg == "pickup":
            msg = await query.message.reply_text("Pickup number:", reply_markup=ForceReply(selective=True))
            context.chat_data["awaiting"] = "pickup"
            context.chat_data["awaiting_id"] = msg.message_id
            context.chat_data["state"] = State.READY
            logger.info(f"Message ID: {msg.message_id} (pickup)")
            return

        if arg == "product":
            product = context.chat_data.get("product")
            logger.info(f"Removing Product: {product}")
            context.chat_data["product"] = Product(created_by = update.effective_user.id, pickup = product.pickup)
            context.chat_data["state"] = State.PRODUCT
            return await query.message.reply_text(
                f"Started new product. Please send pictures of the product.",
                reply_markup = render(context.chat_data["state"], context.chat_data["product"])
            )

    if action == "update":

        if arg == "pickup":
            msg = await query.message.reply_text("Pickup number:", reply_markup=ForceReply(selective=True))
            context.chat_data["awaiting"] = "pickup"
            context.chat_data["awaiting_id"] = msg.message_id
            logger.info(f"Message ID: {msg.message_id} (pickup)")
            return

        if arg == "quantity":
            msg = await query.message.reply_text("Product quantity:", reply_markup=ForceReply(selective=True))
            context.chat_data["awaiting"] = "quantity"
            context.chat_data["awaiting_id"] = msg.message_id
            logger.info(f"Message ID: {msg.message_id} (quantity)")
            return

    # if action == "add_photo_hint":
    #     await q.message.reply_text("Tap the 📎 and send one or more photos of the product.")
    #     return

    # if action == "analyze":
    #     if not session.get("product"):
    #         return
    #     session["state"] = State.ANALYZING
    #     await send_or_edit(update, context)
    #     # Run analysis; when done, move to REVIEW and re-render
    #     await process_product_folder(update, context)
    #     session["state"] = State.REVIEW
    #     return await send_or_edit(update, context)

    # if action == "edit" and arg:
    #     session["awaiting"] = f"edit:{arg}"
    #     msg = await q.message.reply_text(f"Enter new {arg}:", reply_markup=ForceReply(selective=True))
    #     session["await_msg_id"] = msg.message_id
    #     return

    # if action == "confirm":
    #     # TODO: finalize submission here (persist, notify, etc.)
    #     session["state"] = State.DONE
    #     return await send_or_edit(update, context)

    # if action == "back_to_product":
    #     session["state"] = State.PRODUCT
    #     return await send_or_edit(update, context)

    # if action == "cancel_product":
    #     session["product"] = None
    #     session["state"] = State.READY if session.get("pickup") else State.INIT
    #     return await send_or_edit(update, context)

    # await send_or_edit(update, context)

async def on_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):

    awaiting = context.chat_data.get("awaiting")
    awaiting_id = context.chat_data.get("awaiting_id")
    logger.info(f"Replying to ID: {awaiting_id} ({awaiting})")

    if not awaiting:
        return
    if not getattr(update, "message", None) or not update.message.reply_to_message:
        return
    if update.message.reply_to_message.message_id != awaiting_id:
        logger.info(f"Reply Message ID: {update.message.reply_to_message.message_id}")
        return

    text = (update.message.text or "").strip()

    if awaiting == "pickup":
        if text.isdigit():
            context.chat_data["product"].pickup = text
            message = f"Pickup number set to {text}."
        else:
            message = "Pickup must be digits. Try again."

    if awaiting == "quantity":
        if text.isdigit():
            context.chat_data["product"].quantity = text
            message = f"Product quantity set to {text}."
        else:
            message = "Quantity must be a digit. Try again."

    # clear 'awaiting' state
    context.chat_data.pop("awaiting", None)
    context.chat_data.pop("awaiting_id", None)

    markup = render(context.chat_data["state"], context.chat_data["product"])
    return await update.message.reply_text(message, reply_markup = markup)

