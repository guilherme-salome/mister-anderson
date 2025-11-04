#!/usr/bin/env python3

import os
import re
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

authinfo = os.path.join(os.path.expanduser("~"), ".authinfo")


def read_token(machine: str, login: str | None = None) -> str:
    if not os.path.isfile(authinfo):
        raise FileNotFoundError(f"Credential store not found at {authinfo}")

    with open(authinfo) as f:
        for line in f:
            if f"machine {machine}" not in line:
                continue
            if login is not None and f"login {login}" not in line:
                continue
            match = re.search(r'password\s+(\S+)', line)
            if match:
                return match.group(1)

    target = f"machine={machine}" + (f" login={login}" if login else "")
    raise ValueError(f"Credential not found for {target} in {authinfo}")


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
    try:
        read_token('api.openai.com')
    except Exception:
        logger.warning("OpenAI credential not found in %s", authinfo)
    else:
        logger.info("Found OpenAI credential in %s", authinfo)
