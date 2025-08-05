#!/usr/bin/env python3
"""
Hyperliquid Data Pipeline - Main Entry Point

Unified interface to all pipeline functionality:
- Data extraction from various sources
- Database operations and maintenance
- Testing and validation
- Deployment utilities
"""

import argparse
import logging
import time
import structlog
from scripts.core.extractors.transactions_processor import TransactionDataExtractor
from scripts.core.extractors.node_stream_watch import create_ingestion_worker
from scripts.core.extractors.raw_trades_processor import NodeDataExtractor

# Set up logging
logger = structlog.get_logger()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==== Stream Mode ====

def run_node_trades_ingest_loop():
    extractor = NodeDataExtractor()
    worker = create_ingestion_worker(
        base_dir="/data/hyperliquid/hl-data/node_trades/hourly/",
        api_endpoint="localhost:8040/api/hyperliquid_data/transactions/_stream_load",
        log_file="scripts/logs/node_trades_ingest.log",
        checkpoint_file="scripts/data/processed/node_trades_files.json",
        batch_size=1000,
        name="node_trades",
        process_function=extractor.process_node_trades_stream_data
    )
    while True:
        try:
            worker()
        except Exception as e:
            logger.error("Node trades ingest loop error", error=str(e))
        time.sleep(10)

def run_replica_cmds_ingest_loop():
    extractor = TransactionDataExtractor()
    worker = create_ingestion_worker(
        base_dir="/data/hyperliquid/hl-data/replica_cmds/",
        api_endpoint="localhost:8040/api/hyperliquid_data/transactions/_stream_load",
        log_file="scripts/logs/replica_cmds_ingest.log",
        checkpoint_file="scripts/data/processed/replica_cmds_files.json",
        batch_size=1000,
        name="replica_cmds",
        process_function=extractor.process_replica_commands_stream
    )
    while True:
        try:
            worker()
        except Exception as e:
            logger.error("Replica cmds ingest loop error", error=str(e))
        time.sleep(10)

# ==== CLI Entry Point ====

def main():
    parser = argparse.ArgumentParser(description="Hyperliquid Data Pipeline Loop")
    parser.add_argument(
        '--mode',
        choices=[
            'replica_cmds', 'node_trades', 'explorer_blocks', 'node_fills',
            'node_trades_ingest', 'replica_cmds_ingest'
        ],
        required=True,
        help='Which pipeline to run'
    )
    args = parser.parse_args()

    match args.mode:
        case 'node_trades_ingest':
            run_node_trades_ingest_loop()
        case 'replica_cmds_ingest':
            run_replica_cmds_ingest_loop()

if __name__ == "__main__":
    main()