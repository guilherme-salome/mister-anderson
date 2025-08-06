#!/usr/bin/env python3

import sys
import asyncio
import openai

from config import read_token


openai.api_key = read_token('api.openai.com', 'mister-anderson-bot')


async def upload_image(client, path):
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


if __name__ == '__main__':
    async def main():
        images = sys.argv[1:]  # Pass image paths as arguments
        if not images:
            raise Exception("Usage: python llm.py img1.jpg img2.png ...")
        client = openai.AsyncOpenAI(api_key=openai.api_key)
        result = await ask_with_images(client, images, "What do you see?")
        print(result)
    asyncio.run(main())
