#!/usr/bin/env python3
"""
Transaction Data Extractor for Hyperliquid - Enhanced Version with Required Field Validation

This module provides utilities to extract and process:
- Replica Commands: Signed action bundles with nonces, signatures, and action data.
- Explorer Blocks: Block and transaction data from the Hyperliquid explorer.

Enhanced features:
- Comprehensive validation for all required fields as per StarRock schema
- Fallback values for missing required fields
- Detailed tracking of field validation issues
- Robust error handling to prevent batch rejection

Performance optimizations:
- Reduced object allocations and string operations
- Batch processing improvements
- Optimized signature handling
- Cached computations and pre-compiled patterns
- Memory-efficient data structures

Intended for use in Hyperliquid data pipelines and backfill jobs.
"""

import json
import re
from functools import lru_cache
from collections import defaultdict
from ..utils.logging_config import get_pipeline_logger
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
import uuid
from ..utils.signature_utils import rsv_to_signature
from eth_account._utils.signing import to_standard_v
from eth_account.messages import encode_defunct
from eth_account.account import Account
from ..utils.sqlite_helper import insert_with_conflict_resolution

pipeline_logger = get_pipeline_logger(component_name='Transactions', log_level='INFO')
logger = pipeline_logger.get_logger()

# Pre-compile regex patterns for hex validation
HEX_PATTERN = re.compile(r'^0x[0-9a-fA-F]+$')

# Constants to avoid repeated string creation
MISSING_WALLET_TYPE = "MISSING"
USER_WALLET_TYPE = "USER"
VAULT_WALLET_TYPE = "VAULT"
RECOVERED_WALLET_TYPE = "RECOVERED"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
HYPERLIQUID_CHAIN_ID = 999
HYPERLIQUID_EXCHANGE = "Hyperliquid"
DEFAULT_METHOD = "UNKNOWN"
DEFAULT_BLOCK_HASH = "0x0000000000000000000000000000000000000000000000000000000000000000"

# Pre-defined field sets for method processing
ORDER_RECORDED_KEYS = frozenset({"a", "b", "p", "s", "r", "t", "tif", "c"})
CANCEL_RECORDED_KEYS = frozenset({"a", "o"})
CANCEL_BY_CLOID_RECORDED_KEYS = frozenset({"cloid", "asset"})
BRIDGE_RECORDED_KEYS = frozenset({"ethTxHash"})

# Required fields as per StarRock schema
REQUIRED_FIELDS = {
    'id': str,
    'wallet_address': str,
    'method': str,
    'block_timestamp': str,  # DATETIME as string
    'block_number': int,     # BIGINT
    'block_hash': str
}

# Default values for required fields when missing
DEFAULT_VALUES = {
    'id': lambda: str(uuid.uuid4()),
    'wallet_address': ZERO_ADDRESS,
    'method': DEFAULT_METHOD,
    'block_timestamp': lambda: datetime.utcnow().isoformat(),
    'block_number': 0,
    'block_hash': DEFAULT_BLOCK_HASH
}

def safe_json_dumps(obj) -> str:
    """Optimized JSON serialization with reduced function calls."""
    if not obj:
        return '{}'
    
    def convert(obj):
        obj_type = type(obj)
        if obj_type is set:
            return list(obj)
        elif obj_type is dict:
            return {k: convert(v) for k, v in obj.items()}
        elif obj_type is list:
            return [convert(i) for i in obj]
        return obj

    return json.dumps(convert(obj), separators=(',', ':'))  # Compact JSON

@lru_cache(maxsize=1024)
def to_iso_cached(timestamp_str: str) -> str:
    """Cache ISO conversions for repeated timestamps."""
    try:
        if isinstance(timestamp_str, str):
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.isoformat()
    except (ValueError, AttributeError):
        pass
    return timestamp_str

def to_iso(value) -> str:
    """Optimized ISO conversion."""
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, str):
        return to_iso_cached(value)
    return value

def validate_and_fix_required_fields(transaction: dict) -> Tuple[dict, List[str]]:
    """
    Validate and fix required fields in a transaction record.
    
    Args:
        transaction: Transaction dictionary to validate
        
    Returns:
        Tuple of (fixed_transaction, list_of_missing_fields)
    """
    missing_fields = []
    fixed_transaction = transaction.copy()
    
    for field_name, expected_type in REQUIRED_FIELDS.items():
        field_value = fixed_transaction.get(field_name)
        
        # Check if field is missing or None
        if field_value is None:
            missing_fields.append(f"{field_name}: None")
            default_value = DEFAULT_VALUES[field_name]
            fixed_transaction[field_name] = default_value() if callable(default_value) else default_value
            continue
            
        # Check if field is empty string (which would cause issues)
        if isinstance(field_value, str) and not field_value.strip():
            missing_fields.append(f"{field_name}: empty_string")
            default_value = DEFAULT_VALUES[field_name]
            fixed_transaction[field_name] = default_value() if callable(default_value) else default_value
            continue
            
        # Type validation and conversion
        try:
            if expected_type == str:
                fixed_transaction[field_name] = str(field_value)
            elif expected_type == int:
                if isinstance(field_value, str):
                    # Try to convert string to int
                    fixed_transaction[field_name] = int(field_value)
                elif not isinstance(field_value, int):
                    # If it's not int or convertible string, use default
                    missing_fields.append(f"{field_name}: invalid_type_{type(field_value).__name__}")
                    fixed_transaction[field_name] = DEFAULT_VALUES[field_name]
        except (ValueError, TypeError):
            missing_fields.append(f"{field_name}: conversion_error")
            default_value = DEFAULT_VALUES[field_name]
            fixed_transaction[field_name] = default_value() if callable(default_value) else default_value
    
    return fixed_transaction, missing_fields

class TransactionDataExtractor:
    """
    Enhanced extractor for replica commands and explorer blocks from Hyperliquid data sources.
    Now includes comprehensive field validation and fallback handling.
    """

    def __init__(self, output_dir: str = ""):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Pre-allocate reusable objects
        self._uuid_generator = uuid.uuid4
        self._current_timestamp = None
        self._signature_cache = {}  # Cache for signature computations
        
        # Tracking for validation issues
        self.validation_stats = {
            'total_processed': 0,
            'total_with_issues': 0,
            'field_issues': defaultdict(int)
        }
        
        # Method handlers mapping for faster dispatch
        self._method_handlers = {
            "order": self._create_order_txns,
            "cancel": self._create_cancel_txns,
            "cancelByCloid": self._create_cancel_by_cloid_txns,
            "bridge": self._create_bridge_txns,
            "connect": self._create_generic_txns,
            None: self._create_generic_txns
        }

    def _is_valid_hex(self, value: str) -> bool:
        """Fast hex validation using pre-compiled regex."""
        return bool(value and HEX_PATTERN.match(value))

    @lru_cache(maxsize=512)
    def _normalize_hex(self, hex_str: str) -> str:
        """Cached hex normalization."""
        if not hex_str:
            return ""
        return hex_str[2:] if hex_str.startswith("0x") else hex_str

    def _recover_wallet_address(self, action: dict, nonce: str, signature: dict) -> Optional[str]:
        """
        Optimized wallet address recovery with caching and early exits.
        """
        if not signature:
            return None
            
        r = signature.get("r")
        s = signature.get("s")
        v = signature.get("v")

        if not all([r, s, v]):
            return None

        # Create cache key for signature reuse
        cache_key = f"{r}:{s}:{v}:{nonce}"
        if cache_key in self._signature_cache:
            return self._signature_cache[cache_key]

        try:
            # Fast hex validation
            if not (self._is_valid_hex(r) and self._is_valid_hex(s)):
                return None

            r_bytes = bytes.fromhex(self._normalize_hex(r))
            s_bytes = bytes.fromhex(self._normalize_hex(s))
            v_int = to_standard_v(int(v))

            message = encode_defunct(text=f"{nonce}:{action}")
            recovered = Account.recover_message(message, vrs=(v_int, r_bytes, s_bytes))
            
            # Cache the result
            self._signature_cache[cache_key] = recovered
            return recovered
            
        except (ValueError, TypeError) as e:
            logger.debug(f"Wallet recovery failed: {e}")
            self._signature_cache[cache_key] = None
            return None

    def _create_base_transaction(self, common_fields: dict) -> dict:
        """Create base transaction template to reduce dictionary operations."""
        return {
            "asset_id": None,
            "order_id": None,
            "price": None,
            "size": None,
            "is_buy": None,
            "reduce_only": None,
            "time_in_force": None,
            **common_fields
        }

    def _create_order_txns(self, action_data: dict, common_fields: dict, 
                          contract_address: str, action: dict, input_data: str) -> List[dict]:
        """Optimized order transaction creation."""
        orders = action_data.get("orders")
        if not orders:
            return []

        txns = []
        for order in orders:
            order_type = order.get("t", {})
            time_in_force = (
                order_type.get("limit", {}).get("tif") or
                order_type.get("trigger", {}).get("tif")
            )
            
            txn = self._create_base_transaction(common_fields)
            txn.update({
                "contract_address": contract_address,
                "asset_id": order.get("a"),
                "order_id": order.get("c"),
                "is_buy": order.get("b"),
                "price": order.get("p"),
                "size": order.get("s"),
                "reduce_only": order.get("r"),
                "time_in_force": time_in_force,
            })

            # Only add input_data if there are extra keys
            extra_keys = set(order.keys()) - ORDER_RECORDED_KEYS
            if extra_keys:
                txn["input_data"] = input_data

            txns.append(txn)
        return txns

    def _create_cancel_txns(self, action_data: dict, common_fields: dict,
                           contract_address: str, action: dict, input_data: str) -> List[dict]:
        """Optimized cancel transaction creation."""
        cancels = action_data.get("cancels")
        if not cancels:
            return []

        txns = []
        for cancel in cancels:
            txn = self._create_base_transaction(common_fields)
            txn.update({
                "contract_address": contract_address,
                "asset_id": cancel.get("a"),
                "order_id": cancel.get("o"),
            })

            extra_keys = set(cancel.keys()) - CANCEL_RECORDED_KEYS
            if extra_keys:
                txn["input_data"] = input_data

            txns.append(txn)
        return txns

    def _create_cancel_by_cloid_txns(self, action_data: dict, common_fields: dict, contract_address: str, action: dict, input_data: str) -> List[dict]:
        """Optimized cancelByCloid transaction creation."""
        cancels = action_data.get("cancels")
        if not cancels:
            return []

        txns = []
        for cancel in cancels:
            txn = self._create_base_transaction(common_fields)
            txn.update({
                "contract_address": contract_address,
                "asset_id": cancel.get("asset"),
                "order_id": cancel.get("cloid"),
            })

            extra_keys = set(cancel.keys()) - CANCEL_BY_CLOID_RECORDED_KEYS
            if extra_keys:
                txn["input_data"] = input_data

            txns.append(txn)
        return txns

    def _create_bridge_txns(self, action_data: dict, common_fields: dict, contract_address: str, action: dict, input_data: str) -> List[dict]:
        """Optimized bridge transaction creation."""
        txn = self._create_base_transaction(common_fields)
        txn.update({
            "contract_address": action_data.get("ethTxHash"),
            "size": action.get("amount"),
        })

        extra_keys = set(action_data.keys()) - BRIDGE_RECORDED_KEYS
        if extra_keys:
            txn["input_data"] = input_data

        return [txn]

    def _create_generic_txns(self, action_data: dict, common_fields: dict,
                            contract_address: str, action: dict, input_data: str) -> List[dict]:
        """Generic transaction creation for connect and unknown methods."""
        txn = self._create_base_transaction(common_fields)
        txn.update({
            "contract_address": contract_address,
            "input_data": input_data
        })
        return [txn]

    def _create_txns_for_method(self, method: str, action_data: dict, common_fields: dict,
                               contract_address: str, action: dict) -> List[dict]:
        """
        Optimized transaction creation using method handlers.
        Now validates and fixes all transactions before returning.
        """
        input_data = safe_json_dumps(action_data) if action_data else '{}'
        
        # Use handler dispatch instead of if/elif chain
        handler = self._method_handlers.get(method, self._create_generic_txns)
        raw_txns = handler(action_data, common_fields, contract_address, action, input_data)
        
        # Validate and fix all transactions
        validated_txns = []
        total_issues = []
        
        for txn in raw_txns:
            fixed_txn, missing_fields = validate_and_fix_required_fields(txn)
            
            if missing_fields:
                # Update error information for transactions with validation issues
                existing_error = fixed_txn.get("error_message", "")
                validation_error = f"Field validation issues: {', '.join(missing_fields)}"
                
                if existing_error:
                    fixed_txn["error_message"] = f"{existing_error}; {validation_error}"
                else:
                    fixed_txn["error_message"] = validation_error
                    
                total_issues.extend(missing_fields)
            
            # Ensure id is always set uniquely
            if not fixed_txn.get("id"):
                fixed_txn["id"] = str(uuid.uuid4())
                
            validated_txns.append(fixed_txn)
        
        # Update validation stats
        if total_issues:
            self.validation_stats['total_with_issues'] += len(raw_txns)
            for issue in total_issues:
                self.validation_stats['field_issues'][issue] += 1
        
        return validated_txns

    def _handle_validation_issues_batch(self, txns_with_issues: List[dict]) -> None:
        """
        Record transactions that had validation issues to SQLite for monitoring.
        """
        if not txns_with_issues:
            return

        try:
            # Add additional metadata for tracking
            for txn in txns_with_issues:
                txn['id'] = str(self._uuid_generator()) 
                txn["transaction_id"] = txn.get("id")
                if "error_type" not in txn:
                    txn["error_type"] = "field_validation"

            insert_with_conflict_resolution(table_name='transactions',data=txns_with_issues,conflict_columns=['id'],batch_size=1000)
            logger.warning(f"Recorded {len(txns_with_issues)} transactions with validation issues")
        except Exception as e:
            logger.error(f"Failed to record validation issue transactions: {e}", exc_info=True)

    def _format_signature_optimized(self, sig: dict) -> Optional[str]:
        """Optimized signature formatting with caching."""
        r, s, v = sig.get("r"), sig.get("s"), sig.get("v")
        if not all([r, s, v is not None]):
            return None

        cache_key = f"{r}:{s}:{v}"
        if cache_key in self._signature_cache:
            return self._signature_cache[cache_key]

        try:
            r_hex = self._normalize_hex(r).zfill(64)
            s_hex = self._normalize_hex(s).zfill(64)
            v_int = int(v)
            if v_int in (0, 1):
                v_int += 27
            
            final_sig = rsv_to_signature(r_hex, s_hex, v_int)
            self._signature_cache[cache_key] = final_sig
            return final_sig
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to format signature: {e}")
            self._signature_cache[cache_key] = None
            return None

    def process_replica_commands_stream(self, commands: List[Dict]) -> List[Dict]:
        """
        Enhanced processing of replica commands with comprehensive field validation.
        """
        if not commands:
            return []

        transactions = []
        txns_with_validation_issues = []
        
        # Pre-compute current timestamp once
        self._current_timestamp = datetime.utcnow().isoformat()
        
        try:
            # Filter invalid commands once
            valid_cmds = [cmd for cmd in commands if isinstance(cmd, dict) and cmd]
            invalid_count = len(commands) - len(valid_cmds)
            
            if invalid_count > 0:
                logger.error(f"{invalid_count} invalid command(s) found and skipped")

            for cmd in valid_cmds:
                abci_block = cmd.get("abci_block") or {}
                block_time = abci_block.get("time")
                block_number = abci_block.get("round")
                block_hash = abci_block.get("block_hash")
                signed_bundles = abci_block.get("signed_action_bundles") or []
                resps = cmd.get("resps") or {}
                full_response = resps.get("Full") or []

                for n, bundle in enumerate(signed_bundles):
                    if not bundle:
                        continue
                        
                    block_hash_bundle = bundle[0] if bundle else block_hash
                    bundle_data = bundle[1] if len(bundle) > 1 else {}

                    response_pair = full_response[n] if n < len(full_response) else None
                    response_hash = response_pair[0] if response_pair else None
                    response_data = response_pair[1] if response_pair and len(response_pair) > 1 else []

                    signed_actions = bundle_data.get("signed_actions", [])
                    for i, action in enumerate(signed_actions):
                        action_data = action.get("action", {})
                        
                        # Optimized signature processing
                        final_sig = self._format_signature_optimized(action.get("signature", {}))

                        # Optimized wallet address determination
                        wallet_address = (
                            action.get("user") or 
                            action_data.get("user") or 
                            action.get("vaultAddress")
                        )
                        
                        if wallet_address:
                            wallet_type = (
                                USER_WALLET_TYPE if action.get("user") or action_data.get("user")
                                else VAULT_WALLET_TYPE
                            )
                        elif (block_hash_bundle == response_hash and 
                              i < len(response_data) and 
                              response_data[i].get("user")):
                            wallet_address = response_data[i]["user"]
                            wallet_type = USER_WALLET_TYPE
                        else:
                            # Try recovery only if we don't have an address
                            recovered_address = self._recover_wallet_address(
                                action=action_data,
                                nonce=action.get("nonce") or action_data.get("nonce"),
                                signature=action.get("signature", {})
                            )
                            
                            if recovered_address:
                                wallet_address = recovered_address
                                wallet_type = RECOVERED_WALLET_TYPE
                            else:
                                # Use fallback instead of None - this will be handled by validation
                                wallet_address = None  # Will be fixed by validation
                                wallet_type = MISSING_WALLET_TYPE

                        # Pre-build common fields - let validation handle None values
                        common_fields = {
                            "id": str(self._uuid_generator()),
                            "block_timestamp": to_iso(block_time) if block_time else None,
                            "block_number": block_number,
                            "block_hash": block_hash_bundle,
                            "transaction_index": i,
                            "transaction_hash": action.get("raw_tx_hash"),
                            "wallet_address": wallet_address,
                            "wallet_type": wallet_type,
                            "nonce": action.get("nonce") or action_data.get("nonce"),
                            "signature": final_sig,
                            "gas_price": action.get("gasPrice"),
                            "method": action_data.get("type"),  # Will be validated and fixed if None
                            "order_grouping": action_data.get("grouping"),
                            "chain_id": HYPERLIQUID_CHAIN_ID,
                            "exchange": HYPERLIQUID_EXCHANGE if action.get('isFrontend') else None,
                            "agent_info": action.get("agent"),
                            "agent_address": action.get("agentAddress"),
                            "error_message": action.get("error"),
                            "created_at": self._current_timestamp,
                            "updated_at": self._current_timestamp
                        }

                        # Process method-specific transactions (now with validation)
                        method = action_data.get("type")
                        contract_address = action.get("contractAddress")
                        
                        txns_for_method = self._create_txns_for_method(
                            method=method,
                            action_data=action_data,
                            common_fields=common_fields,
                            contract_address=contract_address,
                            action=action
                        )
                        
                        # Separate transactions that had validation issues
                        for txn in txns_for_method:
                            if "Field validation issues:" in str(txn.get("error_message") or ""):
                                txns_with_validation_issues.append(txn)
                        
                        transactions.extend(txns_for_method)

            # Update total processed count
            self.validation_stats['total_processed'] += len(transactions)

            # Batch process transactions with validation issues for monitoring
            if txns_with_validation_issues:
                self._handle_validation_issues_batch(txns_with_validation_issues)

            # Log validation statistics
            if self.validation_stats['total_with_issues'] > 0:
                logger.info(f"Validation Summary:")
                logger.info(f"  Total processed: {self.validation_stats['total_processed']}")
                logger.info(f"  With validation issues: {self.validation_stats['total_with_issues']}")
                logger.info(f"  Field issues breakdown: {dict(self.validation_stats['field_issues'])}")

            logger.info(f"Parsed {len(transactions)} transactions from {len(commands)} command lines.")
            return transactions

        except Exception as e:
            logger.error(f"Error in process_replica_commands_stream: {e}", exc_info=True)
            return []

    def get_validation_stats(self) -> dict:
        """Return current validation statistics."""
        return dict(self.validation_stats)

    def reset_validation_stats(self) -> None:
        """Reset validation statistics."""
        self.validation_stats = {
            'total_processed': 0,
            'total_with_issues': 0,
            'field_issues': defaultdict(int)
        }

    def clear_caches(self) -> None:
        """Clear internal caches to free memory."""
        self._signature_cache.clear()
        to_iso_cached.cache_clear()

def main():
    """Main extraction workflow for replica commands and explorer blocks."""
    pass

if __name__ == "__main__":
    main()