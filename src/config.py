#!/usr/bin/env python3

import os
import re


authinfo = os.path.join(os.path.expanduser("~"), ".authinfo")


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


if __name__ == "__main__":
    if read_token('api.telegram.com'):
        print(f"Found Telegram Token in {authinfo}")
    if read_token('api.openai.com'):
        print(f"Found OpenAI Token in {authinfo}")
