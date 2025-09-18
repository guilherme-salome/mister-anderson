#!/usr/bin/env python3

import os, re, json, logging

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

authinfo = os.path.join(os.path.expanduser("~"), ".authinfo")

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_WHITELIST = os.path.join(PROJECT_DIR, "config", "whitelist.txt")
EXAMPLE_WHITELIST = os.path.join(PROJECT_DIR, "config", "whitelist.example.txt")


def read_token(machine='api.telegram.com', login=None):
    with open(authinfo) as f:
        for line in f:
            if f"machine {machine}" in line:
                if login is not None and f"login {login}" not in line:
                    continue
                m = re.search(r'password\s+(\S+)', line)
                if m:
                    return m.group(1)
    raise ValueError(f"Telegram token not found for machine={machine} and login={login} in {authinfo}")


def read_whitelist(path=None):
    path = (
        path
        or (DEFAULT_WHITELIST if os.path.isfile(DEFAULT_WHITELIST) else EXAMPLE_WHITELIST)
    )
    logger.info(f"Whitelist source: {path}")
    users = set()
    if not path or not os.path.isfile(path):
        return users
    with open(path) as f:
        for line in f:
            m = re.search(r"\b\d+\b", line)
            if m:
                users.add(int(m.group(0)))
    return users

ALLOWED_USERS = read_whitelist()

if __name__ == "__main__":
    if read_token('api.telegram.com'):
        print(f"Found Telegram Token in {authinfo}")
    if read_token('api.openai.com'):
        print(f"Found OpenAI Token in {authinfo}")
    print(read_whitelist())
