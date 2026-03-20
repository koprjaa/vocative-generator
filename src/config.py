# Project: Vocative Generator
# File:    src/config.py
#
# Description:
# Centralizes logging dictConfig, HTTP and backoff tuning, browser user-agents, sklonuj.cz URL, and default CSV filenames.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# Created: 2025-12-14
#
# License: MIT

from typing import List

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'detailed',
            'level': 'INFO'
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True
        },
        'aiohttp': {
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': False
        }
    }
}

HTTP_CONFIG = {
    'TIMEOUT': 15,
    'MAX_RETRIES': 5,
    'INITIAL_DELAY': 0.01,
    'MAX_DELAY': 5,
    'MIN_DELAY': 0.01,
    'BATCH_SIZE': 2000,
    'MAX_WORKERS': 100,
    'MIN_WORKERS': 20,
    'INITIAL_WORKERS': 30,
    'CHECKPOINT_INTERVAL': 5,
    'WORKER_SCALE_INTERVAL': 3,
    'WORKER_SCALE_DOWN_INTERVAL': 3,
    'CHUNK_SIZE': 100000,
    'MEMORY_LIMIT': 0.8,
    'RATE_LIMIT_INITIAL_BACKOFF_SECONDS': 1.0,
    'RATE_LIMIT_BACKOFF_FACTOR_MULTIPLIER': 1.8,
    'RATE_LIMIT_MAX_GLOBAL_BACKOFF_FACTOR': 20.0,
    'SUCCESSES_TO_REDUCE_BACKOFF': 50,
    'COOLDOWN_AFTER_FACTOR_INCREASE_S': 60,
    'BACKOFF_REDUCTION_ON_SUCCESS_RATIO': 0.85,
    'CONNECTION_ERROR_BACKOFF_MULTIPLIER': 1.5,
    'SERVER_ERROR_BACKOFF_MULTIPLIER': 1.8,
    'RATE_LIMIT_BACKOFF_MULTIPLIER': 2.0,
}

USER_AGENTS: List[str] = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
]

API_CONFIG = {
    'BASE_URL': 'https://sklonuj.cz',
    'ENDPOINT': '/generator-osloveni/',
}

FILE_CONFIG = {
    'INPUT_FILE': 'names.csv',
    'OUTPUT_FILE': 'names_with_vocative.csv',
    'CHECKPOINT_FILE': 'checkpoint.json',
    'INPUT_COLUMN_NAME': 'Name'
}
