#!/usr/bin/env python3

import os
import sys
import asyncio
import openai
import logging
import json
import re


from .config import read_token
from .product import Product


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


async def process_product_folder(product: Product):
    logger.info(f"Processing Product with OpenAI: {product}")
    client = openai.AsyncOpenAI(api_key=openai.api_key)

    images = []
    for f in os.listdir(product.tempdir):
        if f.lower().endswith((".jpg", ".jpeg", ".png")):
            images.append(os.path.join(product.tempdir, f))
            logger.info(f"Image Found: {f}")

    if not images:
        logger.info(f"No Images Available for Product: {product}")
        return

    prompt = """
Extract product information from the attached image(s).
Return the answer strictly in JSON format with the following fields:
- serial_number: (string)
- short_description: (string)
- commodity: (string, e.g. laptop, hard drive, etc.)
- destination: (string, e.g., data sanatization, A1 distribution, recycled, TSD)
Example:
{
  "serial_number": "ABC12345",
  "short_description": "Black wireless keyboard with numeric pad",
  "commodity": "keyboard",
  "destination": "A1 distribution",
}
Warning: Only JSON content should be returned. No explanations, no formatting, no code fences.
"""

    description = await ask_with_images(client, images, prompt)
    product.description_raw = description
    logger.info(f"Description: {description}")

    try:
        parsed = json.loads(description)
        logger.info(f"Parsed Description: {parsed}")
    except:
        logger.warning("Description from LLM could not be parsed as JSON.")
    else:
        product.description_json = parsed
    logger.info("Analysis Complete.")
    return product


if __name__ == '__main__':
    async def main():
        images = sys.argv[1:]  # Pass image paths as arguments
        if not images:
            raise Exception("Usage: python llm.py img1.jpg img2.png ...")
        client = openai.AsyncOpenAI(api_key=openai.api_key)
        result = await ask_with_images(client, images, "What do you see?")
        logger.info("LLM response: %s", result)
    asyncio.run(main())
