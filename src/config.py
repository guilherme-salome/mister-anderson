#!/usr/bin/env python3

import os
import re


authinfo = os.path.join(os.path.expanduser("~"), ".authinfo")


def read_token():
    with open(authinfo) as f:
        for line in f:
            if 'api.telegram.com' in line:
                m = re.search(r'password\s+(\S+)', line)
                if m:
                    return m.group(1)
    raise ValueError(f"Telegram token not found in {authinfo}")


if __name__ == "__main__":
    if read_token():
        print(f"Found Telegram Token in {authinfo}")
