import sqlite3
import os
from scripts.core.utils.logging_config import get_pipeline_logger

# -----------------------------
# Setup Database & Logger
# -----------------------------
DB_DIR = "scripts/configs"
os.makedirs(DB_DIR, exist_ok=True)
DB_NAME = os.path.join(DB_DIR, "hyperliquid_data.db")

pipeline_logger = get_pipeline_logger(component_name='SQ Lite', log_level='INFO')
logger = pipeline_logger.get_logger()


def get_connection():
    """Returns a new connection to the SQLite database."""
    return sqlite3.connect(DB_NAME)


def init_db():
    """Initializes the database with required tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    # raw_trades table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_trades (
        id TEXT PRIMARY KEY,
        raw_trade_id TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        token_id TEXT NOT NULL,
        block_timestamp DATETIME NOT NULL,
        trade_hash TEXT NOT NULL,
        block_number INTEGER,
        block_hash TEXT,
        trade_id TEXT,
        wallet_type TEXT,
        amount REAL NOT NULL,
        price REAL NOT NULL,
        side TEXT NOT NULL,
        order_id INTEGER,
        twap_id INTEGER,
        client_order_id TEXT,
        fee_paid REAL,
        fee_token TEXT,
        builder_fee REAL,
        gas_used INTEGER,
        start_position TEXT,
        liquidity_type TEXT,
        cross_type TEXT,
        trade_direction TEXT,
        closed_pnl REAL,
        is_liquidation BOOLEAN,
        liquidation_price REAL,
        liquidation_method TEXT,
        raw_data TEXT,
        error_message TEXT,
        error_type TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # transactions table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        transaction_id TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        method TEXT NOT NULL,
        block_timestamp DATETIME NOT NULL,
        block_number INTEGER NOT NULL,
        block_hash TEXT NOT NULL,
        transaction_hash TEXT,
        transaction_index INTEGER,
        wallet_type TEXT,
        nonce INTEGER,
        signature TEXT,
        input_data TEXT,
        gas_price INTEGER,
        contract_address TEXT,
        exchange TEXT,
        asset_id INTEGER,
        order_id TEXT,
        is_buy BOOLEAN,
        price REAL,
        size REAL,
        reduce_only BOOLEAN,
        time_in_force TEXT,
        order_grouping TEXT,
        chain_id INTEGER,
        agent_info TEXT,
        agent_address TEXT,
        error_message TEXT,
        error_type TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()
    logger.info(f"Database initialized successfully at {DB_NAME}!")

