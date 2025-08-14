#!/usr/bin/env python3
"""
Logging Configuration for Hyperliquid Data Pipeline

Features:
- Structured logging with proper levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Automatic log rotation by size and time
- Logging with buffering
- Centralized configuration for all pipeline components
- Performance monitoring and metrics
- Clean separation of application vs audit logs
"""

import logging
import logging.handlers
import os
import sys
import time
from pathlib import Path
import structlog
from datetime import datetime
import json


class PipelineLogger:
    """
    Logging system for data pipeline operations.
    Implements rotation, compression, and buffering.
    """
    
    def __init__(
        self,
        component_name: str,
        log_dir: str = os.environ.get("LOG_DIR", "/scripts/logs"),
        max_file_size: int = 50 * 1024 * 1024,  # 50MB per file
        backup_count: int = 5,  # Keep 5 backup files
        log_level: str = "INFO"
    ):
        self.component_name = component_name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create separate loggers for different purposes
        self.app_logger = self._setup_application_logger(max_file_size, backup_count, log_level)
        self.perf_logger = self._setup_performance_logger(max_file_size, backup_count)
        self.audit_logger = self._setup_audit_logger(max_file_size, backup_count)
        self.app_logger = self._wrap_logger_for_kwargs(self.app_logger)
        
        # Configure structlog for structured logging
        self._setup_structlog()
        

    def _wrap_logger_for_kwargs(self, logger):
        """
        Make logger methods accept arbitrary kwargs by merging them into the message
        or into `extra`.
        """
        def wrap(level_fn):
            def new_fn(msg, *args, **kwargs):
                # Extract extras (everything except allowed logging keys)
                allowed_keys = {"exc_info", "stack_info", "extra"}
                extras = {k: v for k, v in kwargs.items() if k not in allowed_keys}

                if extras:
                    msg = f"{msg} | extras={json.dumps(extras)}"

                # Keep only allowed kwargs for the actual logger call
                safe_kwargs = {k: v for k, v in kwargs.items() if k in allowed_keys}

                return level_fn(msg, *args, **safe_kwargs)
            return new_fn

        logger.error = wrap(logger.error)
        logger.info = wrap(logger.info)
        logger.debug = wrap(logger.debug)
        logger.warning = wrap(logger.warning)
        logger.critical = wrap(logger.critical)
        return logger

    def _setup_application_logger(self, max_size: int, backup_count: int, level: str) -> logging.Logger:
        """Setup main application logger with rotation."""
        logger = logging.getLogger(f"{self.component_name}.app")
        logger.setLevel(getattr(logging, level.upper()))
        
        # Remove existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Rotating file handler for application logs
        app_file = self.log_dir / f"{self.component_name}_app.log"
        file_handler = logging.handlers.RotatingFileHandler(
            app_file,
            maxBytes=max_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        
        # Console handler for immediate feedback
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _setup_performance_logger(self, max_size: int, backup_count: int) -> logging.Logger:
        """Setup performance metrics logger."""
        logger = logging.getLogger(f"{self.component_name}.perf")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        perf_file = self.log_dir / f"{self.component_name}_performance.log"
        handler = logging.handlers.RotatingFileHandler(
            perf_file,
            maxBytes=max_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        
        # JSON formatter for performance metrics
        formatter = logging.Formatter(
            '%(asctime)s | PERF | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _setup_audit_logger(self, max_size: int, backup_count: int) -> logging.Logger:
        """Setup audit trail logger for data operations."""
        logger = logging.getLogger(f"{self.component_name}.audit")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        audit_file = self.log_dir / f"{self.component_name}_audit.log"
        handler = logging.handlers.RotatingFileHandler(
            audit_file,
            maxBytes=max_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        
        formatter = logging.Formatter(
            '%(asctime)s | AUDIT | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _setup_structlog(self):
        """Configure structlog for structured logging."""
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    
    def get_logger(self) -> logging.Logger:
        """Get the main application logger."""
        return self.app_logger
    
    def log_performance(self, operation: str, duration: float, records_processed: int = 0, **kwargs):
        """Log performance metrics in structured format."""
        metrics = {
            "operation": operation,
            "duration_seconds": round(duration, 3),
            "records_processed": records_processed,
            "records_per_second": round(records_processed / duration, 2) if duration > 0 else 0,
            **kwargs
        }
        
        metric_str = " | ".join([f"{k}={v}" for k, v in metrics.items()])
        self.perf_logger.info(metric_str)
    
    def log_audit(self, action: str, resource: str, status: str, **kwargs):
        """Log audit trail for data operations."""
        audit_data = {
            "action": action,
            "resource": resource,
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            **kwargs
        }
        
        audit_str = " | ".join([f"{k}={v}" for k, v in audit_data.items()])
        self.audit_logger.info(audit_str)
    
    def cleanup_old_logs(self, days_to_keep: int = 7):
        """Clean up old log files beyond retention period."""
        import time
        import glob
        
        cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)
        
        log_patterns = [
            f"{self.component_name}_*.log.*",
            f"{self.component_name}_*_performance.log.*",
            f"{self.component_name}_*_audit.log.*"
        ]
        
        cleaned_count = 0
        for pattern in log_patterns:
            for log_file in glob.glob(str(self.log_dir / pattern)):
                try:
                    if os.path.getctime(log_file) < cutoff_time:
                        os.remove(log_file)
                        cleaned_count += 1
                        self.app_logger.debug(f"Cleaned old log file: {log_file}")
                except OSError as e:
                    self.app_logger.warning(f"Failed to clean log file {log_file}: {e}")
        
        if cleaned_count > 0:
            self.app_logger.info(f"Cleaned {cleaned_count} old log files")

def get_pipeline_logger(component_name: str, log_level: str = "INFO") -> PipelineLogger:
    """Factory function to get a properly configured pipeline logger."""
    return PipelineLogger(component_name, log_level=log_level)

# Context manager for performance logging
class LogPerformance:
    """Context manager for automatic performance logging."""
    
    def __init__(self, logger: PipelineLogger, operation: str, **kwargs):
        self.logger = logger
        self.operation = operation
        self.kwargs = kwargs
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.get_logger().info(f"Starting {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        
        if exc_type is None:
            status = "success"
            level = "info"
        else:
            status = "error"
            level = "error"
        
        self.logger.log_performance(
            self.operation, 
            duration, 
            status=status,
            **self.kwargs
        )
        
        getattr(self.logger.get_logger(), level)(
            f"Completed {self.operation} in {duration:.3f}s (status: {status})"
        )

# Memory monitoring utilities
def log_memory_usage(logger: logging.Logger, operation: str = ""):
    """Log current memory usage for monitoring."""
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        
        logger.info(
            f"Memory usage {operation}: "
            f"RSS={memory_info.rss / 1024 / 1024:.1f}MB, "
            f"VMS={memory_info.vms / 1024 / 1024:.1f}MB"
        )
    except ImportError:
        logger.debug("psutil not available for memory monitoring")
    except Exception as e:
        logger.warning(f"Failed to log memory usage: {e}")

def monitor_disk_space(logger: logging.Logger, path: str = "/scripts/logs", threshold_gb: float = 1.0):
    """Monitor disk space and log warnings if low."""
    try:
        import shutil
        total, used, free = shutil.disk_usage(path)
        
        free_gb = free / (1024**3)
        total_gb = total / (1024**3)
        used_percent = (used / total) * 100
        
        if free_gb < threshold_gb:
            logger.warning(
                f"Low disk space on {path}: "
                f"{free_gb:.1f}GB free ({used_percent:.1f}% used)"
            )
        else:
            logger.debug(
                f"Disk space {path}: "
                f"{free_gb:.1f}GB free of {total_gb:.1f}GB ({used_percent:.1f}% used)"
            )
            
    except Exception as e:
        logger.warning(f"Failed to monitor disk space for {path}: {e}")