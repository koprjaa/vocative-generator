# Project: Vocative Generator
# File:    main.py
#
# Description:
# Orchestrates chunked CSV ingestion, adaptive batch processing, checkpoint persistence, and graceful shutdown for bulk vocative generation.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# Created: 2025-11-01
#
# License: MIT

import asyncio
import logging
import logging.config
import time
import signal
import pandas as pd
from pathlib import Path
from typing import Tuple

from src.config import LOGGING_CONFIG, HTTP_CONFIG, FILE_CONFIG, USER_AGENTS
from src.adapters import AdaptiveDelay, AdaptiveWorkers, AdaptiveBatchSize, UserAgentManager
from src.services import CheckpointService, NameService, BatchService
from src.utils import GracefulShutdownHandler, create_aiohttp_session

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def _ensure_input_csv_or_dummy(input_file: str) -> None:
    if Path(input_file).exists():
        return
    logger.error(f"Input file {input_file} not found. Exiting.")
    logger.info(f"Creating dummy input file {input_file} for testing.")
    pd.DataFrame({
        'ID': range(1, 21),
        FILE_CONFIG['INPUT_COLUMN_NAME']: [f'First Name Last Name {i}' for i in range(1, 21)],
    }).to_csv(input_file, index=False, encoding='utf-8')


def _persist_loaded_checkpoint_to_disk(checkpoint_service: CheckpointService) -> None:
    if not checkpoint_service.processed_names:
        return
    logger.info(f"Flushing checkpoint before start: {len(checkpoint_service.processed_names)} processed names.")
    checkpoint_service.save_checkpoint(
        checkpoint_service.last_chunk_fully_processed_index,
        checkpoint_service.last_batch_completed_for_current_chunk,
        {},
        is_chunk_complete=checkpoint_service.last_batch_completed_for_current_chunk == 0
    )
    logger.info("Checkpoint flushed successfully before start.")


def _estimate_total_csv_rows(input_file: str) -> int:
    try:
        total_rows_in_file = sum(1 for _ in pd.read_csv(input_file, chunksize=10000, usecols=[0])) - 1
        logger.info(f"Total records to process in {input_file}: {total_rows_in_file:,}")
        return total_rows_in_file
    except Exception as e:
        logger.warning(f"Could not determine total rows in input file: {e}")
        return -1


def _initialize_output_csv_for_run(checkpoint_service: CheckpointService, output_file: str) -> None:
    if checkpoint_service.last_chunk_fully_processed_index == -1 and checkpoint_service.last_batch_completed_for_current_chunk == 0:
        logger.info(f"Creating new output file {output_file} with headers.")
        pd.DataFrame(columns=['ID', FILE_CONFIG['INPUT_COLUMN_NAME'], 'Vocative', 'Vocative First Name', 'Vocative Last Name', 'Error']).to_csv(output_file, index=False, encoding='utf-8')
        return
    logger.info(f"Appending to existing output file {output_file} or resuming.")


async def _process_one_input_chunk(
    chunk_df: pd.DataFrame,
    current_chunk_index: int,
    total_processed_rows: int,
    shutdown_handler: GracefulShutdownHandler,
    checkpoint_service: CheckpointService,
    batch_service: BatchService,
    output_file: str,
    total_rows_in_file: int,
) -> Tuple[int, bool]:
    if shutdown_handler.shutdown_requested:
        logger.info("Shutdown requested, stopping further chunk processing.")
        return total_processed_rows, True

    logger.info(f"\n--- Processing Chunk {current_chunk_index} ({len(chunk_df):,} records) ---")

    if checkpoint_service.last_chunk_fully_processed_index >= current_chunk_index:
        logger.info(f"Chunk {current_chunk_index} was already fully processed. Skipping.")
        return total_processed_rows + len(chunk_df), False

    if FILE_CONFIG['INPUT_COLUMN_NAME'] not in chunk_df.columns:
        logger.error(f"Chunk {current_chunk_index} does not contain '{FILE_CONFIG['INPUT_COLUMN_NAME']}' column. Skipping.")
        return total_processed_rows, False

    if 'ID' not in chunk_df.columns:
        logger.warning(f"Chunk {current_chunk_index} does not contain 'ID' column. Generating sequential IDs for this chunk.")
        chunk_df['ID'] = range(total_processed_rows + 1, total_processed_rows + len(chunk_df) + 1)

    original_count_in_chunk = len(chunk_df)
    name_col = FILE_CONFIG['INPUT_COLUMN_NAME']

    chunk_df = chunk_df.dropna(subset=[name_col]).copy()
    chunk_df[name_col] = chunk_df[name_col].astype(str).str.strip()
    chunk_df = chunk_df[chunk_df[name_col] != '']

    cleaned_count_in_chunk = len(chunk_df)
    if original_count_in_chunk != cleaned_count_in_chunk:
        logger.info(f"Cleaned chunk {current_chunk_index}: removed {original_count_in_chunk - cleaned_count_in_chunk} empty/NaN '{name_col}'s.")

    if cleaned_count_in_chunk == 0:
        logger.info(f"Chunk {current_chunk_index} is empty after cleaning. Skipping.")
        return total_processed_rows, False

    for col in ['Vocative', 'Vocative First Name', 'Vocative Last Name', 'Error']:
        if col not in chunk_df.columns: chunk_df[col] = pd.NA

    await batch_service.process_chunk_data(chunk_df, current_chunk_index)

    if shutdown_handler.shutdown_requested or checkpoint_service.last_chunk_fully_processed_index < current_chunk_index:
        logger.info(f"Appending processed chunk {current_chunk_index} to {output_file}...")
        chunk_df.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8')
        logger.info(f"Chunk {current_chunk_index} successfully appended.")

    total_processed_rows += len(chunk_df)

    actual_success_count = (
        chunk_df['Vocative'].notna()
        & (chunk_df['Vocative'] != '')
        & (chunk_df['Vocative'].str.lower() != chunk_df[name_col].str.lower())
    ).sum()
    logger.info(f"--- Chunk {current_chunk_index} Finished ---")
    logger.info(f"- Processed in chunk: {len(chunk_df):,} records")
    logger.info(f"- Successfully transformed: {actual_success_count:,} names ({actual_success_count / len(chunk_df) * 100:.1f}%)")
    if total_rows_in_file > 0:
        logger.info(
            f"- Overall progress: {total_processed_rows:,}/{total_rows_in_file:,} records "
            f"({total_processed_rows / total_rows_in_file * 100:.1f}%)"
        )
    else:
        logger.info(f"- Overall processed: {total_processed_rows:,} records")

    return total_processed_rows, False


async def _cancel_main_task_if_shutdown(signaled: bool, main_task: asyncio.Task) -> None:
    if not signaled:
        return
    logger.info("Shutdown event triggered. Cancelling main processing task if not done.")
    if not main_task.done():
        main_task.cancel()
        try:
            await main_task
        except asyncio.CancelledError:
            logger.info("Main processing task successfully cancelled after shutdown signal.")


def _log_main_task_result_if_finished(main_task: asyncio.Task, done: set) -> None:
    if main_task not in done:
        return
    try:
        main_task.result()
        logger.info("Main processing task completed.")
    except asyncio.CancelledError:
        logger.info("Main processing task was cancelled (as expected or externally).")
    except Exception as e:
        logger.error(f"Main processing task failed with an exception: {e}", exc_info=True)


async def _cancel_all_outstanding_tasks(loop: asyncio.AbstractEventLoop) -> None:
    logger.info("Main_wrapper finishing. Performing final cleanup of tasks...")
    tasks_to_cancel = [task for task in asyncio.all_tasks(loop=loop) if task is not asyncio.current_task() and not task.done()]
    if not tasks_to_cancel:
        logger.info("No outstanding tasks to cancel.")
        logger.info("Application shutdown sequence complete.")
        return
    logger.info(f"Cancelling {len(tasks_to_cancel)} outstanding tasks...")
    for task in tasks_to_cancel:
        task.cancel()
    await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    logger.info("Outstanding tasks processed.")
    logger.info("Application shutdown sequence complete.")


async def process_all_names_main_task(shutdown_handler: GracefulShutdownHandler):
    start_time_total = time.time()
    logger.info("Application starting name processing...")

    input_file = FILE_CONFIG['INPUT_FILE']
    output_file = FILE_CONFIG['OUTPUT_FILE']

    _ensure_input_csv_or_dummy(input_file)

    delay_adapter = AdaptiveDelay(
        initial_delay=HTTP_CONFIG['INITIAL_DELAY'],
        max_delay=HTTP_CONFIG['MAX_DELAY'],
        min_delay=HTTP_CONFIG['MIN_DELAY']
    )
    worker_adapter = AdaptiveWorkers(
        initial_workers=HTTP_CONFIG.get('INITIAL_WORKERS', HTTP_CONFIG['MIN_WORKERS']),
        max_workers=HTTP_CONFIG['MAX_WORKERS'],
        min_workers=HTTP_CONFIG['MIN_WORKERS']
    )
    user_agent_manager = UserAgentManager(USER_AGENTS)
    batch_size_adapter = AdaptiveBatchSize(
        initial_size=HTTP_CONFIG['BATCH_SIZE'],
        max_size=HTTP_CONFIG.get('MAX_BATCH_SIZE', HTTP_CONFIG['BATCH_SIZE'] * 2),
        min_size=HTTP_CONFIG.get('MIN_BATCH_SIZE', max(10, HTTP_CONFIG['BATCH_SIZE'] // 4))
    )
    checkpoint_service = CheckpointService()

    _persist_loaded_checkpoint_to_disk(checkpoint_service)

    async with create_aiohttp_session() as session:
        name_service = NameService(session, user_agent_manager)
        batch_service = BatchService(
            name_service, checkpoint_service,
            delay_adapter, worker_adapter, batch_size_adapter,
            shutdown_handler.shutdown_event
        )

        total_processed_rows = 0
        current_chunk_index = -1

        total_rows_in_file = _estimate_total_csv_rows(input_file)

        _initialize_output_csv_for_run(checkpoint_service, output_file)

        try:
            for chunk_df in pd.read_csv(input_file, chunksize=HTTP_CONFIG['CHUNK_SIZE'], encoding='utf-8'):
                current_chunk_index += 1
                total_processed_rows, stop = await _process_one_input_chunk(
                    chunk_df,
                    current_chunk_index,
                    total_processed_rows,
                    shutdown_handler,
                    checkpoint_service,
                    batch_service,
                    output_file,
                    total_rows_in_file,
                )
                if stop:
                    break

        except asyncio.CancelledError:
            logger.warning("Main processing task was cancelled. Saving checkpoint if possible...")
            raise

        except Exception as e:
            logger.critical(f"Critical error during chunk processing: {e}", exc_info=True)
            raise

    total_time = time.time() - start_time_total
    logger.info(f"=== Total Processing Finished ===")
    logger.info(f"Total execution time: {total_time:.2f} seconds")
    logger.info(f"Total records processed across all chunks: {total_processed_rows:,}")


async def main_wrapper():
    shutdown_handler = GracefulShutdownHandler()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_handler.handle_signal, sig, None)
        except NotImplementedError:
            # Windows: add_signal_handler is not supported on ProactorEventLoop.
            # Fall back to signal.signal() for SIGINT; SIGTERM is not delivered on Windows.
            if sig == signal.SIGINT:
                signal.signal(sig, lambda s, f: shutdown_handler.handle_signal(s, f))

    main_task = None
    try:
        logger.info("Starting main_wrapper...")
        main_task = asyncio.create_task(process_all_names_main_task(shutdown_handler))
        shutdown_task = asyncio.create_task(shutdown_handler.shutdown_event.wait())

        done = (await asyncio.wait([main_task, shutdown_task], return_when=asyncio.FIRST_COMPLETED))[0]

        await _cancel_main_task_if_shutdown(shutdown_handler.shutdown_event.is_set(), main_task)

        _log_main_task_result_if_finished(main_task, done)

    except asyncio.CancelledError:
        logger.info("Main_wrapper task itself was cancelled.")
        if main_task and not main_task.done():
            main_task.cancel()
            await asyncio.gather(main_task, return_exceptions=True)

    except Exception as e:
        logger.critical(f"Unhandled critical error in main_wrapper: {e}", exc_info=True)

    finally:
        await _cancel_all_outstanding_tasks(loop)


if __name__ == "__main__":
    try:
        asyncio.run(main_wrapper())
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt caught directly in __main__. Application will exit.")
    except Exception as e:
        logger.critical(f"Critical error at top level: {e}", exc_info=True)
    finally:
        logging.shutdown()
        print("Application has exited.")
