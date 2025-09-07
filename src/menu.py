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
            [InlineKeyboardButton(f"🚚 Pickup: {product.pickup}", callback_data="act:set:pickup")],
            [InlineKeyboardButton("📝 New Product", callback_data="act:set:product")],
        ]

    if state == State.PRODUCT:
        kb = [
            [InlineKeyboardButton(f"🚚 Pickup: {product.pickup}", callback_data="act:set:pickup")],
            [InlineKeyboardButton(f"📝 Product Tag: {product.asset_tag}", callback_data="noop:set:tag")],
            [InlineKeyboardButton(f"📦 Quantity: {product.quantity}", callback_data="act:set:quantity")],
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
            logger.info(f"Message ID: {msg.message_id} (pickup)")
            return

        if arg == "product":
            product = context.chat_data.get("product")
            pickup = product.pickup
            logger.info(f"Removing Product: {product}")
            context.chat_data["product"] = Product(created_by = update.effective_user.id)
            context.chat_data["state"] = State.PRODUCT
            return await query.message.reply_text(
                f"Started new product. Please send pictures of the product.",
                reply_markup = render(context.chat_data["state"], context.chat_data["product"])
            )


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

# async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     query = update.callback_query
#     await query.answer()
#     data = query.data
#     logger.info(f"Button: {data}")
#     # Handle button click
#     if data == "kbd_pickup":
#         await handle_kbd_pickup(update, context)
#         return
#     elif data == "kbd_new":
#         await handle_kbd_new(update, context)
#         return
#     elif data == "kbd_photo":
#         await handle_kbd_photo(update, context)
#         return
#     elif data == "kbd_analyze":
#         await process_product_folder(update, context)
#         # asyncio.create_task(process_product_folder(context.chat_data["product"], context))
#     # elif data == "kbd_quantity":
#     #     await handle_kbd_quantity(update, context)
#     #     return
#     # elif data == "kbd_end":
#     #     asyncio.create_task(process_product_folder(context.chat_data["product"], context))
#     #     await query.message.reply_text(f"Ended product for Pickup {pickup_number}.")
#     product = context.chat_data.get("product")
#     markup = get_menu(product["pickup"], product)
#     await query.message.reply_text("Please choose:", reply_markup=markup)

# async def handle_kbd_pickup(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     query = update.callback_query
#     await query.answer()
#     msg = await query.message.reply_text(
#         "Pickup number:",
#         reply_markup=ForceReply(selective=True)
#     )
#     logger.info(f"Message ID: {msg.message_id}")
#     context.chat_data["kbd_pickup_prompt_id"] = msg.message_id


# async def pickup_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     prompt_id = context.chat_data.get("kbd_pickup_prompt_id")
#     logger.info(f"Reply - Message ID: {prompt_id}")

#     if not prompt_id:
#         return  # no active prompt, ignore

#     if not update.message.reply_to_message:
#         return  # not a reply at all, ignore

#     if update.message.reply_to_message.message_id != prompt_id:
#         return  # reply, but not to our prompt, ignore

#     # Now we know it's a valid pickup reply
#     number = update.message.text.strip()
#     if not number.isdigit():
#         await update.message.reply_text(
#             "Pickup number can only have digits, please try again.",
#             reply_markup=render()
#         )
#         return

#     context.chat_data["pickup"] = number
#     await update.message.reply_text(
#         f"Pickup number set to {number}.",
#         reply_markup=render(number)
#     )

#     # Clear the prompt ID so random replies don’t trigger again
#     context.chat_data.pop("kbd_pickup_prompt_id", None)


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
            context.chat_data["state"] = State.READY
        else:
            await update.message.reply_text("Pickup must be digits. Try again.")
            return

        # clear 'awaiting' state
        context.chat_data.pop("awaiting", None)
        context.chat_data.pop("awaiting_id", None)

        markup = render(context.chat_data["state"], context.chat_data["product"])
        return await update.message.reply_text(f"Pickup number set to {text}.", reply_markup = markup)

# async def handle_kbd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     query = update.callback_query
#     await query.answer()
#     product = start_new_product(update, context)
#     context.chat_data["product"] = product
#     await query.message.reply_text(
#         f"Started new product. Please send pictures of the product.",
#         reply_markup = get_menu(product["pickup"], product)
#     )


# async def handle_kbd_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     query = update.callback_query
#     await query.answer()
#     msg = await query.message.reply_text(
#         "Now please tap the 📎 (attachment) icon and take or select one or more photos of the product!",
#         reply_markup=ForceReply(selective=True)
#     )

# async def handle_kbd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     query = update.callback_query
#     await query.answer()
#     product = start_new_product(update, context)
#     context.chat_data["product"] = product
#     await query.message.reply_text(
#         f"Started new product. Please send pictures of the product.",
#         reply_markup = get_menu(product["pickup"], product)
#     )



# async def handle_kbd_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     return
