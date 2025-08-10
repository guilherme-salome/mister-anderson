#!/usr/bin/env python3

import os
import sys
import asyncio
import openai
import logging

from .config import read_token


logger = logging.getLogger(__name__)

openai.api_key = read_token('api.openai.com', 'mister-anderson-bot')


async def upload_image(client, path):
    logger.info(f"Uploading Image to OpenAI: {path}")
    with open(path, "rb") as f:
        result = await client.files.create(file=f, purpose="vision")
    return result.id


async def ask_with_images(client, images, prompt):
    ids = [await upload_image(client, path) for path in images]
    response = await client.responses.create(
        model="gpt-4.1-mini",
        input=[{
            "role": "user",
            "content": [
                {"type": "input_text", "text": prompt},
                *[
                    {"type": "input_image", "file_id": fid}
                    for fid in ids
                ]
            ]
        }],
    )
    return response.output_text


async def process_product_folder(product, context):
    logger.info(f"Processing Product with OpenAI: {product}")
    client = openai.AsyncOpenAI(api_key=openai.api_key)

    images = []
    for f in os.listdir(product["path"]):
        if f.lower().endswith((".jpg", ".jpeg", ".png")):
            images.append(os.path.join(product["path"], f))

    if not images:
        logger.info(f"No Images Available for Product: {product}")
        return

    prompt = "Describe this product and list technical specifications."
    description = await ask_with_images(client, images, prompt)
    specs_path = os.path.join(product["path"], "specs.txt")
    with open(specs_path, "w") as f:
        f.write(description)
    if product["chat_id"]:
        await context.bot.send_message(
            chat_id=product["chat_id"],
            text=f"Product description and specifications have been generated!\n{description}"
        )


if __name__ == '__main__':
    async def main():
        images = sys.argv[1:]  # Pass image paths as arguments
        if not images:
            raise Exception("Usage: python llm.py img1.jpg img2.png ...")
        client = openai.AsyncOpenAI(api_key=openai.api_key)
        result = await ask_with_images(client, images, "What do you see?")
        print(result)
    asyncio.run(main())
