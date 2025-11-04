#!/usr/bin/env python3

import os
import sys
import asyncio
import json
import logging
import re
import textwrap
from typing import Dict, Iterable, List, Optional, Sequence

import openai

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


def _prepare_prompt(
    *,
    subcategory_options: Optional[Sequence[str]] = None,
    destiny_options: Optional[Sequence[Dict[str, object]]] = None,
) -> str:
    subcategory_list = list(subcategory_options or [])
    destiny_list = list(destiny_options or [])

    subcategory_json = json.dumps(subcategory_list[:80], ensure_ascii=False)
    subcategory_more = max(len(subcategory_list) - 80, 0)

    destiny_map: Dict[str, object] = {str(item["code"]): item["label"] for item in destiny_list if "code" in item and "label" in item}
    destiny_json = json.dumps(destiny_map, ensure_ascii=False)

    guidance_lines: List[str] = []
    guidance_lines.append("Always choose a destination code from the provided list. If you cannot determine a match, pick the closest option and explain why in destination_reason.")
    guidance_lines.append("Items such as phones, tablets, usb drives, documents or hard drives MUST go to DATA SANITIZATION, which has code 6.")
    guidance_lines.append("Prefer subcategories from the provided list. Suggest a new subcategory only when none of the provided values fit.")

    prompt = textwrap.dedent(
        f"""
        Extract product information from the attached image(s) and respond with strict JSON (no code fences, no commentary).
        Required JSON keys:
          - serial_number: string
          - short_description: short marketing-ready description
          - subcategory: value from `subcategory_options` when possible
          - cod_destiny: integer destination code
          - destination_label: matching text label for the selected destination
          - destination_reason: concise explanation (<=20 words) of why this destination fits
          - asset_tag: string (leave empty if unknown)

        Context for classification:
          • subcategory_options (sample): {subcategory_json}{" (+" + str(subcategory_more) + " more)" if subcategory_more else ""}
          • destination_options: {destiny_json}

        Rules:
          - {" ".join(guidance_lines)}
          - Use uppercase for destination_label exactly as provided in destination_options.
          - If information is not visible, return an empty string for that field (do not use placeholder text).

        Example response:
        {{
          "serial_number": "ABC12345",
          "short_description": "Dell Latitude 7490 14-inch laptop",
          "subcategory": "Laptop",
          "cod_destiny": 6,
          "destination_label": "DATA SANITIZATION",
          "destination_reason": "Contains storage that must be wiped",
          "asset_tag": ""
        }}
        """
    ).strip()
    return prompt


async def process_product_folder(
    product: Product,
    *,
    subcategory_options: Optional[Sequence[str]] = None,
    destiny_options: Optional[Sequence[Dict[str, object]]] = None,
) -> Product:
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

    prompt = _prepare_prompt(
        subcategory_options=subcategory_options,
        destiny_options=destiny_options,
    )
    description = await ask_with_images(client, images, prompt)
    product.description_raw = description
    logger.info(f"Description: {description}")

    try:
        parsed = json.loads(description)
        logger.info(f"Parsed Description: {parsed}")
    except:
        logger.warning("Description from LLM could not be parsed as JSON.")
    else:
        if isinstance(parsed, dict) and "commodity" in parsed and "subcategory" not in parsed:
            parsed["subcategory"] = parsed.pop("commodity")
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
