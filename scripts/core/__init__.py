"""
Hyperliquid Data Pipeline Core Module

This module contains the core data processing functionality for the Hyperliquid pipeline:
- Signature utilities for ECDSA conversion
- Data extractors for various Hyperliquid data sources  
- Database management utilities
"""

from .utils.signature_utils import rsv_to_signature, signature_to_rsv, validate_signature_format
from .utils.sqlite_helper import get_connection, insert_raw_trades, insert_transactions

__version__ = "1.0.0"
__all__ = [
    "rsv_to_signature",
    "signature_to_rsv", 
    "validate_signature_format",
    'get_connection', 
    'insert_raw_trades',
    'insert_transactions'
] 