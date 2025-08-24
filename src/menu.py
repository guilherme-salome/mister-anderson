#!/usr/bin/env python3

import logging


from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.ext import ContextTypes


from .llm import process_product_folder
from .product import start_new_product


logger = logging.getLogger(__name__)


def get_menu(pickup=None, product=None):
    logger.info(f"Pickup: {pickup}")
    logger.info(f"Product: {product}")
    keyboard = [[
        InlineKeyboardButton(
            "🚚 Pickup: " + (str(pickup) if pickup else "<not set>"),
            callback_data="kbd_pickup"
        )
    ]]
    if pickup is not None and product is None:
        logger.info(f"Menu: New Product")
        keyboard.append(
            [
                InlineKeyboardButton("📝 New Product", callback_data="kbd_new"),
            ]
        )
    if pickup is not None and product is not None:
        keyboard.append([
            InlineKeyboardButton(
                f"📷 Send Photo ({len(product['photos'])})",
                callback_data="kbd_photo"
            )
        ])
        # keyboard.append([InlineKeyboardButton("📦 Quantity", callback_data="kbd_quantity")])
        keyboard.append([InlineKeyboardButton("🤖 Analyze Product", callback_data="kbd_analyze")])

        # keyboard.append([InlineKeyboardButton("✅ Submit Product", callback_data="kbd_end")])
    return InlineKeyboardMarkup(keyboard)


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    logger.info(f"Button: {data}")
    # Handle button click
    if data == "kbd_pickup":
        await handle_kbd_pickup(update, context)
        return
    elif data == "kbd_new":
        await handle_kbd_new(update, context)
        return
    elif data == "kbd_photo":
        await handle_kbd_photo(update, context)
        return
    elif data == "kbd_analyze":
        await process_product_folder(update, context)
        # asyncio.create_task(process_product_folder(context.chat_data["product"], context))
    # elif data == "kbd_quantity":
    #     await handle_kbd_quantity(update, context)
    #     return
    # elif data == "kbd_end":
    #     asyncio.create_task(process_product_folder(context.chat_data["product"], context))
    #     await query.message.reply_text(f"Ended product for Pickup {pickup_number}.")
    product = context.chat_data.get("product")
    markup = get_menu(product["pickup"], product)
    await query.message.reply_text("Please choose:", reply_markup=markup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    markup = get_menu(context.chat_data.get("pickup"))
    message = """
Hello! I’m Mr. Anderson, your assistant for processing deliveries.

I can help you quickly and accurately enter product details after a pickup by analyzing images and generating clear product descriptions.
Just send me a product photo to get started, and I’ll do the rest!

Please choose:
    """
    context.chat_data["pickup"] = None
    await update.message.reply_text(message, reply_markup=markup)


async def handle_kbd_pickup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg = await query.message.reply_text(
        "Pickup number:",
        reply_markup=ForceReply(selective=True)
    )
    logger.info(f"Message ID: {msg.message_id}")
    context.chat_data["kbd_pickup_prompt_id"] = msg.message_id


async def pickup_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prompt_id = context.chat_data.get("kbd_pickup_prompt_id")
    logger.info(f"Reply - Message ID: {prompt_id}")

    if not prompt_id:
        return  # no active prompt, ignore

    if not update.message.reply_to_message:
        return  # not a reply at all, ignore

    if update.message.reply_to_message.message_id != prompt_id:
        return  # reply, but not to our prompt, ignore

    # Now we know it's a valid pickup reply
    number = update.message.text.strip()
    if not number.isdigit():
        await update.message.reply_text(
            "Pickup number can only have digits, please try again.",
            reply_markup=get_menu()
        )
        return

    context.chat_data["pickup"] = number
    await update.message.reply_text(
        f"Pickup number set to {number}.",
        reply_markup=get_menu(number)
    )

    # Clear the prompt ID so random replies don’t trigger again
    context.chat_data.pop("kbd_pickup_prompt_id", None)


async def handle_kbd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    product = start_new_product(update, context)
    context.chat_data["product"] = product
    await query.message.reply_text(
        f"Started new product. Please send pictures of the product.",
        reply_markup = get_menu(product["pickup"], product)
    )


async def handle_kbd_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg = await query.message.reply_text(
        "Now please tap the 📎 (attachment) icon and take or select one or more photos of the product!",
        reply_markup=ForceReply(selective=True)
    )

async def handle_kbd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    product = start_new_product(update, context)
    context.chat_data["product"] = product
    await query.message.reply_text(
        f"Started new product. Please send pictures of the product.",
        reply_markup = get_menu(product["pickup"], product)
    )



async def handle_kbd_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return
