import os
import json
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone
from typing import Callable, Dict, List, Generator, Optional, Tuple
import structlog
from itertools import islice
import uuid
from functools import lru_cache
import mmap
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from ..utils.logging_config import get_pipeline_logger

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

# Constants
DEFAULT_CHUNK_SIZE = 1048576  # 1MB for better I/O performance with large files
DEFAULT_MAX_BUFFER_SIZE = 50 * 1024 * 1024  # 50MB buffer for large JSON objects
DEFAULT_BATCH_SIZE = 1000  # Increased for large files
MAX_RECORDS_PER_REQUEST = 250000
HTTP_TIMEOUT = (30, 300)  # Increased read timeout
MAX_RETRIES = 3

class OptimizedJSONParser:
    """Optimized JSON parser with better memory management and error recovery."""
    
    def __init__(self, chunk_size: int = DEFAULT_CHUNK_SIZE, max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE):
        self.chunk_size = chunk_size
        self.max_buffer_size = max_buffer_size
        self.decoder = json.JSONDecoder()
        self._stats = {
            'objects_parsed': 0,
            'bytes_processed': 0,
            'parse_errors': 0
        }
    
    def get_stats(self) -> Dict:
        return self._stats.copy()
    
    def _try_parse_line_json(self, line: str) -> Optional[Dict]:
        """Fast path for line-delimited JSON."""
        line = line.strip()
        if not line or not (line.startswith('{') or line.startswith('[')):
            return None
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None
    
    def _find_json_boundaries(self, buffer: str, start_pos: int = 0) -> Tuple[int, int]:
        """Find the boundaries of the next JSON object."""
        brace_count = 0
        bracket_count = 0
        in_string = False
        escape_next = False
        
        i = start_pos
        start_found = False
        actual_start = start_pos
        
        while i < len(buffer):
            char = buffer[i]
            
            if escape_next:
                escape_next = False
            elif char == '\\' and in_string:
                escape_next = True
            elif char == '"' and not escape_next:
                in_string = not in_string
            elif not in_string:
                if char == '{':
                    if not start_found:
                        actual_start = i
                        start_found = True
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                elif char == '[':
                    if not start_found:
                        actual_start = i
                        start_found = True
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
                
                # Check if we've closed all braces/brackets
                if start_found and brace_count == 0 and bracket_count == 0:
                    return actual_start, i + 1
            
            i += 1
        
        return -1, -1
    
    def stream_json_objects(self, file_path: str) -> Generator[Dict, None, None]:
        """
        Optimized streaming JSON parser with better error recovery and memory usage.
        """
        try:
            file_size = os.path.getsize(file_path)
            self._stats['bytes_processed'] = 0
            
            # Use memory mapping for large files
            if file_size > 100 * 1024 * 1024:  # 100MB
                yield from self._stream_with_mmap(file_path, file_size)
            else:
                yield from self._stream_with_buffered_read(file_path)
                
        except Exception as e:
            print(f"Error in stream_json_objects: {e}")
            return
    
    def _stream_with_mmap(self, file_path: str, file_size: int) -> Generator[Dict, None, None]:
        """Memory-mapped streaming for large files."""
        with open(file_path, 'r', encoding='utf-8') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                buffer = ""
                chunk_start = 0
                
                while chunk_start < file_size:
                    chunk_end = min(chunk_start + self.chunk_size, file_size)
                    chunk = mm[chunk_start:chunk_end].decode('utf-8', errors='replace')
                    buffer += chunk
                    
                    # Process complete JSON objects
                    yield from self._process_buffer(buffer)
                    
                    # Update position
                    chunk_start = chunk_end
                    self._stats['bytes_processed'] = chunk_end
    
    def _stream_with_buffered_read(self, file_path: str) -> Generator[Dict, None, None]:
        """Standard buffered reading for smaller files."""
        buffer = ""
        
        with open(file_path, 'r', encoding='utf-8') as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                
                buffer += chunk
                self._stats['bytes_processed'] += len(chunk)
                
                # Process buffer and get remaining content
                remaining_buffer = yield from self._process_buffer(buffer)
                buffer = remaining_buffer
                
                # Memory management - prevent buffer from growing too large
                if len(buffer) > self.max_buffer_size:
                    buffer = buffer[-self.chunk_size:]  # Keep only recent data
        
        # Process any remaining buffer content
        if buffer.strip():
            yield from self._process_remaining_buffer(buffer)
    
    def _process_buffer(self, buffer: str) -> str:
        """Process JSON objects from buffer, return remaining content."""
        processed_up_to = 0
        
        while True:
            buffer_slice = buffer[processed_up_to:].lstrip()
            if not buffer_slice:
                break
            
            # Adjust processed_up_to for stripped content
            lstrip_offset = len(buffer[processed_up_to:]) - len(buffer_slice)
            processed_up_to += lstrip_offset
            
            # Try line-delimited JSON first (fast path)
            newline_pos = buffer_slice.find('\n')
            if newline_pos != -1:
                line = buffer_slice[:newline_pos]
                obj = self._try_parse_line_json(line)
                if obj is not None:
                    self._stats['objects_parsed'] += 1
                    yield obj
                    processed_up_to += newline_pos + 1
                    continue
            
            # Try to find JSON object boundaries
            start_pos, end_pos = self._find_json_boundaries(buffer_slice)
            if start_pos == -1:
                break  # No complete JSON object found
            
            json_str = buffer_slice[start_pos:end_pos]
            try:
                obj = json.loads(json_str)
                self._stats['objects_parsed'] += 1
                yield obj
                processed_up_to += start_pos + end_pos
            except json.JSONDecodeError:
                self._stats['parse_errors'] += 1
                # Skip this malformed object
                processed_up_to += start_pos + 1
        
        return buffer[processed_up_to:]
    
    def _process_remaining_buffer(self, buffer: str) -> Generator[Dict, None, None]:
        """Process remaining buffer content at end of file."""
        buffer = buffer.strip()
        if not buffer:
            return
        
        # Try to parse as complete JSON
        try:
            obj = json.loads(buffer)
            self._stats['objects_parsed'] += 1
            yield obj
        except json.JSONDecodeError:
            # Try line by line
            for line in buffer.split('\n'):
                obj = self._try_parse_line_json(line)
                if obj is not None:
                    self._stats['objects_parsed'] += 1
                    yield obj


class HTTPSessionManager:
    """Optimized HTTP session with connection pooling and retry logic."""

    def __init__(self):
        self.session = requests.Session()

        retry_strategy = Retry(
            total=MAX_RETRIES,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"HEAD", "GET", "PUT", "POST", "DELETE", "OPTIONS", "TRACE"}),
            backoff_factor=1
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
            pool_block=True
        )

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Install the safe default-timeout wrapper
        self.session.request = self._request_with_timeout

    def _request_with_timeout(self, method, url, **kwargs):
        # Only set a timeout if the caller didn't provide one
        if "timeout" not in kwargs:
            kwargs["timeout"] = HTTP_TIMEOUT
        # Call the real Session.request
        return requests.Session.request(self.session, method, url, **kwargs)

    def close(self):
        self.session.close()
    
    def close(self):
        """Close the session and clean up connections."""
        self.session.close()

def create_ingestion_worker(
    base_dir: str,
    api_endpoint: str,
    log_file: str,
    checkpoint_file: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    process_function: Callable[[List[Dict]], List[Dict]] = None,
    name: str = "ingestor",
    failed_batch_handler: Callable[[list, str], None] = None,
    index_column_name: str = '',
    file_chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE,
    parallel_files: int = 1,  # Number of files to process in parallel
    enable_progress_logging: bool = True
) -> Callable[[], None]:

    pipeline_logger = get_pipeline_logger(component_name=name, log_level='INFO')
    logger = pipeline_logger.get_logger()
    
    # Thread-safe logging
    log_lock = threading.Lock()
    
    # HTTP session manager
    session_manager = HTTPSessionManager()
    
    # Basic Auth
    auth = HTTPBasicAuth('root', '')

    # Ensure log file directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    @lru_cache(maxsize=1000)
    def get_formatted_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    def write_to_file(msg: str) -> None:
        try:
            with log_lock:
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(msg + "\n")
        except Exception as e:
            print(f"[log_file] Failed to write to log file: {e}")

    def log_info(event: str, **kwargs):
        message = f"[{get_formatted_timestamp()}] [{name}] {event} {json.dumps(kwargs, separators=(',', ':'))}"
        write_to_file(message)
        if enable_progress_logging:
            logger.info(event, **kwargs)

    def log_error(event: str, **kwargs):
        message = f"[{get_formatted_timestamp()}] [{name}] ERROR: {event} {json.dumps(kwargs, separators=(',', ':'))}"
        write_to_file(message)
        logger.error(event, **kwargs)

    def load_checkpoint() -> Dict[str, bool]:
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                log_error("Failed to load checkpoint", error=str(e))
        return {}

    def save_checkpoint(cp: Dict[str, bool]) -> None:
        try:
            cp_dir = os.path.dirname(checkpoint_file)
            if cp_dir:
                os.makedirs(cp_dir, exist_ok=True)
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(cp, f, separators=(',', ':'))
        except Exception as e:
            log_error("Failed to save checkpoint", error=str(e))

    def chunked(iterable, size):
        """Optimized chunking with iterator reuse."""
        it = iter(iterable)
        while True:
            chunk = list(islice(it, size))
            if not chunk:
                break
            yield chunk

    def handle_failed_starrocks_txns(
        failed_txns: List[Dict],
        index_column_name: str,
        reason: str = "",
    ):
        """Handle transactions that failed to be sent to StarRocks."""
        if not failed_txns:
            return

        if not index_column_name or not isinstance(index_column_name, str):
            log_error("Invalid index_column_name", value=index_column_name)
            return

        error_type = "starrocks_failed"
        error_message = reason or "Failed to insert into StarRocks"

        for txn in failed_txns:
            new_id = str(uuid.uuid4())
            txn["id"] = new_id

            # Only set index_column_name if it doesn't already exist
            if index_column_name not in txn or not txn[index_column_name]:
                txn[index_column_name] = new_id

            txn.setdefault("error_type", error_type)
            txn.setdefault("error_message", error_message)

        if callable(failed_batch_handler):
            try:
                failed_batch_handler(failed_txns, 1000)
            except Exception as e:
                log_error(
                    "Failed batch handler error",
                    error=str(e),
                    count=len(failed_txns),
                )
        else:
            log_error("No failed batch handler", count=len(failed_txns))

    def send_batch_optimized(data_batch: List[Dict]) -> bool:
        """Optimized batch sending with better error handling."""
        if not data_batch:
            return True
            
        total_records = len(data_batch)
        chunks_sent = 0
        total_chunks = (total_records + MAX_RECORDS_PER_REQUEST - 1) // MAX_RECORDS_PER_REQUEST
        
        log_info(f"Sending {total_records} records in {total_chunks} chunks")

        start_time = time.time()
        
        for chunk_index, chunk in enumerate(chunked(data_batch, MAX_RECORDS_PER_REQUEST)):
            try:
                label = f"load_{uuid.uuid4().hex}"

                headers = {
                    "label": label,
                    "format": "json",
                    "strip_outer_array": "true",
                    "Content-Type": "application/json",
                    "ignore_json_size": "true",
                    "Expect": "100-continue",
                    "max_filter_ratio": "0.1"
                }

                # Use optimized JSON serialization
                payload = json.dumps(chunk, separators=(',', ':'))
                
                response = session_manager.session.put(
                    api_endpoint,
                    data=payload,
                    headers=headers,
                    auth=auth,
                    timeout=HTTP_TIMEOUT
                )

                if response.status_code != 200:
                    error_msg = f"StarRocks returned {response.status_code}: {response.text[:500]}"
                    log_error(
                        "Failed to send chunk",
                        chunk_index=chunk_index,
                        status=response.status_code,
                        error=error_msg
                    )
                    handle_failed_starrocks_txns(failed_txns=chunk, index_column_name=index_column_name, reason=error_msg)
                
                chunks_sent += 1
                if chunk_index % 2 == 0:
                    log_info(f"{chunk_index} chunks have been recorded successfully")

            except Exception as e:
                error_msg = f"Exception during API request: {str(e)}"
                log_error(
                    "Exception during API request for chunk",
                    chunk_index=chunk_index,
                    error=error_msg
                )
                handle_failed_starrocks_txns(failed_txns=chunk, index_column_name=index_column_name, reason=error_msg)
                return False

        elapsed = time.time() - start_time
        throughput = total_records / elapsed if elapsed > 0 else 0
        
        log_info("All records sent successfully", 
                records=total_records, 
                chunks=chunks_sent,
                elapsed_seconds=round(elapsed, 2),
                records_per_second=round(throughput, 2))
        return True

    def process_file_optimized(path: str, rel_path: str, checkpoint: Dict[str, bool]) -> bool:
        """Optimized file processing with better error handling."""
        log_info("Starting to process file", file=rel_path)
        
        # Check if file has already been processed
        if checkpoint.get(rel_path, False):
            log_info("File already processed, skipping", file=rel_path)
            return True

        try:
            file_size = os.path.getsize(path)
            log_info("Processing file", file=rel_path, size_mb=round(file_size / (1024*1024), 2))
            
        except Exception as e:
            log_error("Failed to access file", file=rel_path, error=str(e))
            return False

        if file_size == 0:
            log_info("Empty file, skipping", file=rel_path)
            checkpoint[rel_path] = True
            save_checkpoint(checkpoint)
            return True

        # Initialize optimized parser
        parser = OptimizedJSONParser(file_chunk_size, max_buffer_size)
        
        try:
            batch = []
            total_objects = 0
            batch_num = 1
            start_time = time.time()
            
            # Stream JSON objects and process in batches
            for json_obj in parser.stream_json_objects(path):
                batch.append(json_obj)
                total_objects += 1
                
                # Process batch when it reaches the specified size
                if len(batch) >= batch_size:
                    if not process_and_send_batch(batch, rel_path, batch_num, process_function):
                        return False
                    
                    batch = []
                    batch_num += 1
            
            # Process remaining objects in the final batch
            if batch:
                if not process_and_send_batch(batch, rel_path, batch_num, process_function):
                    return False
            
            # Get parser statistics
            stats = parser.get_stats()
            elapsed = time.time() - start_time
            throughput = total_objects / elapsed if elapsed > 0 else 0
            
            # Mark file as fully processed
            checkpoint[rel_path] = True
            save_checkpoint(checkpoint)
            
            log_info("File successfully processed", 
                    file=rel_path, 
                    total_objects=total_objects, 
                    total_batches=batch_num,
                    parse_errors=stats['parse_errors'],
                    elapsed_seconds=round(elapsed, 2),
                    objects_per_second=round(throughput, 2))
            
            return True
            
        except Exception as e:
            log_error("Failed to process JSON file", file=rel_path, error=str(e))
            return False

    def process_and_send_batch(batch: List[Dict], rel_path: str, batch_num: int, 
                              process_func: Callable) -> bool:
        """Process and send a batch with error handling."""
        try:
            log_info("Processing batch", file=rel_path, batch_num=batch_num, batch_size=len(batch))
            
            if process_func:
                processed_batch = process_func(batch)
            else:
                processed_batch = batch
            
            if processed_batch:
                if send_batch_optimized(processed_batch):
                    log_info("Batch successfully processed and sent", 
                           file=rel_path, batch_num=batch_num, records=len(processed_batch))
                    return True
                else:
                    log_error("Failed to send batch", file=rel_path, batch_num=batch_num)
                    return False
            else:
                log_info("Process function returned empty batch", file=rel_path, batch_num=batch_num)
                return True
                
        except Exception as e:
            log_error("Failed to process batch", file=rel_path, batch_num=batch_num, error=str(e))
            return False

    def worker_loop() -> None:
        """Main worker loop with optimizations."""
        log_info("Starting optimized JSON ingestion worker")

        resolved_base = os.path.abspath(os.path.expanduser(base_dir))
        if not os.path.exists(resolved_base):
            log_error("Base directory not found", path=resolved_base)
            return

        checkpoint = load_checkpoint()
        log_info("Loaded checkpoint", checkpoint_entries=len(checkpoint))

        # Collect all files first
        all_files = []
        for root, dirs, files in os.walk(resolved_base):
            for file_name in sorted(files):
                full_path = os.path.join(root, file_name)
                if os.path.isfile(full_path):
                    rel_path = os.path.relpath(full_path, resolved_base)
                    all_files.append((full_path, rel_path))

        total_files = len(all_files)
        processed_files = 0
        failed_files = 0
        
        log_info("Found files to process", total_files=total_files)

        # Process files (with optional parallelization)
        if parallel_files > 1:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=parallel_files) as executor:
                future_to_file = {
                    executor.submit(process_file_optimized, full_path, rel_path, checkpoint): (full_path, rel_path)
                    for full_path, rel_path in all_files
                }
                
                for future in as_completed(future_to_file):
                    full_path, rel_path = future_to_file[future]
                    try:
                        success = future.result()
                        if success:
                            processed_files += 1
                        else:
                            failed_files += 1
                    except Exception as e:
                        log_error("Error processing file", file=rel_path, error=str(e))
                        failed_files += 1
        else:
            # Sequential processing
            for full_path, rel_path in all_files:
                try:
                    if process_file_optimized(full_path, rel_path, checkpoint):
                        processed_files += 1
                    else:
                        failed_files += 1
                except Exception as e:
                    log_error("Error processing file", file=rel_path, error=str(e))
                    failed_files += 1

        log_info("JSON ingestion completed", 
                total_files=total_files, 
                processed_files=processed_files,
                failed_files=failed_files)
        
        # Clean up
        session_manager.close()

    return worker_loop