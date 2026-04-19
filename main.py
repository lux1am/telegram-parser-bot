#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio

from assistant import assistant_main
from bot import bot_main


async def main() -> None:
    await asyncio.gather(
        bot_main(),
        assistant_main(),
    )


if __name__ == "__main__":
    asyncio.run(main())
