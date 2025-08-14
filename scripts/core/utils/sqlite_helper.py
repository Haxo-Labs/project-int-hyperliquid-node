import sqlite3
from contextlib import contextmanager
from typing import List, Dict, Union, Optional, Any, Tuple
from functools import lru_cache, wraps
import threading
import time
from scripts.configs.db_config import get_connection, init_db
from scripts.core.utils.logging_config import get_pipeline_logger

pipeline_logger = get_pipeline_logger(component_name='SQLite', log_level='INFO')
logger = pipeline_logger.get_logger()

# Constants for optimization
DEFAULT_BATCH_SIZE = 1000
MAX_RETRIES = 3
RETRY_DELAY = 0.1  # 100ms
WAL_MODE_PRAGMAS = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL", 
    "PRAGMA cache_size=10000",
    "PRAGMA temp_store=memory",
    "PRAGMA mmap_size=268435456"  # 256MB
]

# Thread-local storage for connections
_thread_local = threading.local()

class SQLiteOptimizer:
    """Handles SQLite connection optimization and caching."""
    
    def __init__(self):
        self._table_schemas = {}
        self._prepared_statements = {}
        self._lock = threading.Lock()
    
    @lru_cache(maxsize=100)
    def get_table_columns(self, table_name: str) -> Tuple[str, ...]:
        """Cache table schema information."""
        try:
            with get_optimized_connection() as conn:
                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                columns = tuple(row[1] for row in cursor.fetchall())
                return columns
        except Exception as e:
            logger.error(f"Failed to get table columns for {table_name}: {e}")
            return ()
    
    @lru_cache(maxsize=50)
    def get_insert_statement(self, table_name: str, columns: Tuple[str, ...]) -> str:
        """Generate and cache INSERT statements."""
        if not columns:
            return ""
        
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['?'] * len(columns))
        return f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    def clear_cache(self):
        """Clear all caches."""
        self.get_table_columns.cache_clear()
        self.get_insert_statement.cache_clear()

# Global optimizer instance
_optimizer = SQLiteOptimizer()

@contextmanager
def get_optimized_connection():
    """
    Get an optimized SQLite connection with WAL mode and performance settings.
    Uses thread-local storage for connection reuse.
    """
    # Check if we have a connection in thread-local storage
    if not hasattr(_thread_local, 'connection') or _thread_local.connection is None:
        _thread_local.connection = get_connection()
        
        # Apply optimization pragmas
        cursor = _thread_local.connection.cursor()
        for pragma in WAL_MODE_PRAGMAS:
            try:
                cursor.execute(pragma)
            except Exception as e:
                logger.warning(f"Failed to apply pragma '{pragma}': {e}")
        cursor.close()
    
    try:
        yield _thread_local.connection
    except Exception:
        # Close connection on error to force fresh connection next time
        if hasattr(_thread_local, 'connection') and _thread_local.connection:
            _thread_local.connection.close()
            _thread_local.connection = None
        raise

def retry_on_database_error(max_retries: int = MAX_RETRIES, delay: float = RETRY_DELAY):
    """Decorator to retry database operations on lock/busy errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
                    last_exception = e
                    error_msg = str(e).lower()
                    
                    # Retry on specific SQLite errors
                    if any(err in error_msg for err in ['locked', 'busy', 'database is locked']):
                        if attempt < max_retries:
                            logger.warning(f"Database locked, retrying in {delay}s (attempt {attempt + 1}/{max_retries + 1})")
                            time.sleep(delay * (2 ** attempt))  # Exponential backoff
                            continue
                    
                    # Re-raise if not a retryable error
                    raise
            
            # If we get here, all retries failed
            raise last_exception
        return wrapper
    return decorator

def validate_data_structure(data: Union[Dict, List[Dict]], operation: str) -> List[Dict]:
    """Validate and normalize input data structure."""
    if not data:
        logger.info(f"No data provided for {operation}")
        return []

    # Normalize to list
    if isinstance(data, dict):
        data = [data]
    elif not isinstance(data, list):
        raise ValueError(f"Invalid data type for {operation}: expected dict or list")

    # Validate all items are dictionaries
    normalized_data = []
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            logger.warning(f"Skipping invalid item at index {i}: not a dictionary")
            continue
        if not item:  # Skip empty dictionaries
            logger.warning(f"Skipping empty item at index {i}")
            continue
        normalized_data.append(item)

    return normalized_data

def prepare_batch_data(data: List[Dict], columns: Tuple[str, ...]) -> List[Tuple]:
    """Prepare data for batch insertion with proper column alignment."""
    prepared_data = []
    
    for item in data:
        # Extract values in column order, using None for missing columns
        row = tuple(item.get(col) for col in columns)
        prepared_data.append(row)
    
    return prepared_data

def log_performance_metrics(operation: str, record_count: int, start_time: float, table_name: str):
    """Log performance metrics for database operations."""
    elapsed = time.time() - start_time
    rate = record_count / elapsed if elapsed > 0 else 0
    
    logger.info(
        f"{operation} completed",
        table=table_name,
        records=record_count,
        elapsed_seconds=round(elapsed, 3),
        records_per_second=round(rate, 1)
    )

@retry_on_database_error()
def bulk_insert_optimized(
    table_name: str, 
    data: List[Dict], 
    batch_size: int = DEFAULT_BATCH_SIZE,
    ignore_duplicates: bool = False
) -> int:
    """
    Optimized bulk insert with batching and performance monitoring.
    
    Args:
        table_name: Target table name
        data: List of dictionaries to insert
        batch_size: Number of records per batch
        ignore_duplicates: If True, use INSERT OR IGNORE
        
    Returns:
        Number of records successfully inserted
    """
    if not data:
        return 0
    
    start_time = time.time()
    total_inserted = 0
    
    # Get table schema
    columns = _optimizer.get_table_columns(table_name)
    if not columns:
        raise ValueError(f"Table '{table_name}' not found or has no columns")
    
    # Get prepared statement
    base_statement = _optimizer.get_insert_statement(table_name, columns)
    if ignore_duplicates:
        statement = base_statement.replace("INSERT INTO", "INSERT OR IGNORE INTO")
    else:
        statement = base_statement
    
    with get_optimized_connection() as conn:
        cursor = conn.cursor()
        
        # Process data in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_data = prepare_batch_data(batch, columns)
            
            try:
                cursor.executemany(statement, batch_data)
                total_inserted += cursor.rowcount
            except Exception as e:
                logger.error(f"Failed to insert batch {i//batch_size + 1}: {e}")
                # Continue with next batch instead of failing completely
                continue
        
        conn.commit()
    
    log_performance_metrics("Bulk insert", total_inserted, start_time, table_name)
    return total_inserted

@retry_on_database_error()
def upsert_data(
    table_name: str,
    data: List[Dict],
    conflict_columns: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """
    Perform UPSERT (INSERT OR REPLACE) operation.
    
    Args:
        table_name: Target table name
        data: List of dictionaries to upsert
        conflict_columns: Columns to check for conflicts
        batch_size: Number of records per batch
        
    Returns:
        Number of records processed
    """
    if not data:
        return 0
    
    start_time = time.time()
    total_processed = 0
    
    columns = _optimizer.get_table_columns(table_name)
    if not columns:
        raise ValueError(f"Table '{table_name}' not found")
    
    # Build UPSERT statement
    columns_str = ', '.join(columns)
    placeholders = ', '.join(['?'] * len(columns))
    update_clause = ', '.join([f"{col} = excluded.{col}" for col in columns if col not in conflict_columns])
    
    statement = f"""
    INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})
    ON CONFLICT({', '.join(conflict_columns)}) DO UPDATE SET {update_clause}
    """
    
    with get_optimized_connection() as conn:
        cursor = conn.cursor()
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_data = prepare_batch_data(batch, columns)
            
            try:
                cursor.executemany(statement, batch_data)
                total_processed += len(batch)
            except Exception as e:
                logger.error(f"Failed to upsert batch {i//batch_size + 1}: {e}")
                continue
        
        conn.commit()
    
    log_performance_metrics("Upsert", total_processed, start_time, table_name)
    return total_processed

# Optimized main insertion functions
def insert_raw_trades(trades: Union[Dict, List[Dict]], batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """
    Optimized insertion for raw_trades table.
    
    Args:
        trades: Single dict or list of dicts to insert
        batch_size: Records per batch for large datasets
        
    Returns:
        Number of records inserted
    """
    try:
        normalized_trades = validate_data_structure(trades, "raw trades insertion")
        if not normalized_trades:
            return 0
        
        return bulk_insert_optimized("raw_trades", normalized_trades, batch_size)
        
    except Exception as e:
        logger.error(f"Failed to insert raw trades: {e}", exc_info=True)
        return 0

def insert_transactions(transactions: Union[Dict, List[Dict]], batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """
    Optimized insertion for transactions table.
    
    Args:
        transactions: Single dict or list of dicts to insert
        batch_size: Records per batch for large datasets
        
    Returns:
        Number of records inserted
    """
    try:
        normalized_transactions = validate_data_structure(transactions, "transactions insertion")
        if not normalized_transactions:
            return 0
        
        return bulk_insert_optimized("transactions", normalized_transactions, batch_size)
        
    except Exception as e:
        logger.error(f"Failed to insert transactions: {e}", exc_info=True)
        return 0

def insert_with_conflict_resolution(
    table_name: str,
    data: Union[Dict, List[Dict]],
    conflict_columns: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """
    Insert data with automatic conflict resolution (upsert).
    
    Args:
        table_name: Target table
        data: Data to insert
        conflict_columns: Columns that define uniqueness
        batch_size: Batch size for processing
        
    Returns:
        Number of records processed
    """
    try:
        normalized_data = validate_data_structure(data, f"{table_name} upsert")
        if not normalized_data:
            return 0
        
        return upsert_data(table_name, normalized_data, conflict_columns, batch_size)
        
    except Exception as e:
        logger.error(f"Failed to upsert data into {table_name}: {e}", exc_info=True)
        return 0

@retry_on_database_error()
def execute_query(query: str, params: Optional[Tuple] = None) -> List[Dict]:
    """
    Execute a SELECT query and return results as list of dictionaries.
    
    Args:
        query: SQL query to execute
        params: Query parameters
        
    Returns:
        List of result dictionaries
    """
    try:
        with get_optimized_connection() as conn:
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Convert rows to dictionaries
            results = [dict(row) for row in cursor.fetchall()]
            return results
            
    except Exception as e:
        logger.error(f"Failed to execute query: {e}", exc_info=True)
        return []

def get_table_stats(table_name: str) -> Dict[str, Any]:
    """Get statistics for a table."""
    try:
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        size_query = f"SELECT page_count * page_size as size FROM pragma_page_count('{table_name}'), pragma_page_size"
        
        with get_optimized_connection() as conn:
            cursor = conn.cursor()
            
            # Get row count
            cursor.execute(count_query)
            row_count = cursor.fetchone()[0]
            
            # Get approximate size (this is a rough estimate)
            cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size")
            total_db_size = cursor.fetchone()[0]
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'estimated_size_bytes': total_db_size // 10,  # Rough estimate
                'estimated_size_mb': round((total_db_size // 10) / (1024 * 1024), 2)
            }
            
    except Exception as e:
        logger.error(f"Failed to get stats for table {table_name}: {e}")
        return {'error': str(e)}

def cleanup_connections():
    """Clean up thread-local connections and caches."""
    if hasattr(_thread_local, 'connection') and _thread_local.connection:
        _thread_local.connection.close()
        _thread_local.connection = None
    
    _optimizer.clear_cache()

# Context manager for batch operations
@contextmanager
def batch_operation():
    """Context manager for batch database operations with optimization."""
    try:
        logger.debug("Starting batch operation")
        yield
    finally:
        logger.debug("Batch operation completed")

# Initialize database on import
try:
    init_db()
    logger.info("SQLite helper initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize SQLite helper: {e}")