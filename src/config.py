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


def read_basic_users(machine='mister-anderson-webui'):
    users = {}
    if not os.path.isfile(authinfo):
        return users
    with open(authinfo) as f:
        for line in f:
            if f"machine {machine}" in line:
                m_login = re.search(r'login\s+(\S+)', line)
                m_pass = re.search(r'password\s+(\S+)', line)
                if m_login and m_pass:
                    users[m_login.group(1)] = m_pass.group(1)
    return users


if __name__ == "__main__":
    if read_token('api.telegram.com'):
        logger.info("Found Telegram Token in %s", authinfo)
    if read_token('api.openai.com'):
        logger.info("Found OpenAI Token in %s", authinfo)
    logger.debug("Whitelist contents: %s", read_whitelist())
