# Project: Vocative Generator
# File:    src/utils.py
#
# Description:
# Implements SIGINT/SIGTERM-driven shutdown coordination and builds the aiohttp ClientSession used for sklonuj.cz requests.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# Created: 2025-12-14
#
# License: MIT

import signal
import asyncio
import logging
import aiohttp
from aiohttp import TCPConnector, ClientTimeout
from contextlib import asynccontextmanager
from .config import HTTP_CONFIG

class GracefulShutdownHandler:
    def __init__(self):
        self.shutdown_requested = False
        self.shutdown_event = asyncio.Event()
        self.logger = logging.getLogger(self.__class__.__name__)

    def handle_signal(self, signum, frame):
        if not self.shutdown_requested:
            self.logger.warning(f"Received signal {signal.Signals(signum).name}. Requesting graceful shutdown...")
            self.shutdown_requested = True
            self.shutdown_event.set()
        else:
            self.logger.warning("Shutdown already in progress.")

@asynccontextmanager
async def create_aiohttp_session():
    # Pool sized for peak concurrency; traffic is single-host so per-host cap matches worker cap.
    connector = TCPConnector(
        ssl=False,
        limit=HTTP_CONFIG['MAX_WORKERS'] * 2,
        limit_per_host=HTTP_CONFIG['MAX_WORKERS']
    )
    # Session-level ceiling slightly above NameService per-request timeout to avoid races on slow responses.
    timeout_config = ClientTimeout(total=HTTP_CONFIG['TIMEOUT'] + 5)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout_config,
        skip_auto_headers=['User-Agent']
    ) as session:
        yield session
