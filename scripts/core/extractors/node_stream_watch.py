import os
import json
import requests
from datetime import datetime, timezone
from typing import Callable, Dict, List
import structlog

# === Structlog Setup ===
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(pad_event=0),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

def create_ingestion_worker(
    base_dir: str,
    api_endpoint: str,
    log_file: str,
    checkpoint_file: str,
    batch_size: int,
    process_function: Callable[[List[Dict]], List[Dict]],
    name: str = "ingestor"
) -> Callable[[], None]:
    # Ensure log file directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    def write_to_file(msg: str) -> None:
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception as e:
            print(f"[log_file] Failed to write to log file: {e}")

    logger = structlog.get_logger(name)

    def log_info(event: str, **kwargs):
        message = f"[{datetime.now(timezone.utc).isoformat()}] [{name}] {event} {json.dumps(kwargs)}"
        print(message)
        write_to_file(message)
        logger.info(event, **kwargs)

    def log_error(event: str, **kwargs):
        message = f"[{datetime.now(timezone.utc).isoformat()}] [{name}] ERROR: {event} {json.dumps(kwargs)}"
        print(message)
        write_to_file(message)
        logger.error(event, **kwargs)

    def load_checkpoint() -> Dict[str, int]:
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                log_error("Failed to load checkpoint", error=str(e))
        return {}

    def save_checkpoint(cp: Dict[str, int]) -> None:
        try:
            cp_dir = os.path.dirname(checkpoint_file)
            if cp_dir:
                os.makedirs(cp_dir, exist_ok=True)
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(cp, f)
        except Exception as e:
            log_error("Failed to save checkpoint", error=str(e))

    def send_batch(data_batch: List[Dict]) -> bool:
        try:
            response = requests.post(api_endpoint, json=data_batch, timeout=10)
            if response.status_code == 200:
                return True
            else:
                log_error("Failed to send batch", status=response.status_code, response=response.text)
                return False
        except Exception as e:
            log_error("Exception during API request", error=str(e))
            return False

    def process_file(path: str, rel_path: str, checkpoint: Dict[str, int]) -> None:
        processed_lines = checkpoint.get(rel_path, 0)

        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except Exception as e:
            log_error("Failed to read file", file=rel_path, error=str(e))
            return

        new_lines = lines[processed_lines:]
        if not new_lines:
            log_info("No new lines to process", file=rel_path)
            return

        log_info("Processing file", file=rel_path, new_lines=len(new_lines))

        for i in range(0, len(new_lines), batch_size):
            batch = new_lines[i:i + batch_size]
            try:
                json_batch = [json.loads(line.strip()) for line in batch if line.strip()]
                processed_batch = process_function(json_batch)
            except Exception as e:
                log_error("Failed to process batch", file=rel_path, error=str(e))
                continue

            if processed_batch and send_batch(processed_batch):
                processed_lines += len(batch)
                checkpoint[rel_path] = processed_lines
                save_checkpoint(checkpoint)
                log_info("Batch successfully sent", file=rel_path, records=len(processed_batch))
            else:
                log_info("Batch failed to send, will retry later", file=rel_path)
                break  # stop and retry this file next run

    def worker_loop() -> None:
        log_info("Starting ingestion worker")

        resolved_base = os.path.abspath(os.path.expanduser(base_dir))
        if not os.path.exists(resolved_base):
            log_error("Base directory not found", path=resolved_base)
            return

        checkpoint = load_checkpoint()

        for root, dirs, files in os.walk(resolved_base):
            for file_name in sorted(files):
                full_path = os.path.join(root, file_name)
                rel_path = os.path.relpath(full_path, resolved_base)

                if os.path.isfile(full_path):
                    process_file(full_path, rel_path, checkpoint)

        log_info("Ingestion scan completed")

    return worker_loop