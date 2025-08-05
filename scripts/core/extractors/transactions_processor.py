#!/usr/bin/env python3
"""
Transaction Data Extractor for Hyperliquid

This module provides utilities to extract and process:
- Replica Commands: Signed action bundles with nonces, signatures, and action data.
- Explorer Blocks: Block and transaction data from the Hyperliquid explorer.

Features:
- Extraction and normalization of replica command and explorer block data.
- Batch insertion utilities for StarRocks.
- Field path extraction for schema exploration and debugging.

Intended for use in Hyperliquid data pipelines and backfill jobs.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import uuid
from ..utils.signature_utils import rsv_to_signature

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def safe_json_dumps(obj):
    def convert(obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(i) for i in obj]
        return obj

    return json.dumps(convert(obj))

def to_iso(value):
    return value.isoformat() if isinstance(value, datetime) else value

class TransactionDataExtractor:
    """
    Extracts and processes replica commands and explorer blocks from Hyperliquid data sources.
    """

    def __init__(self, output_dir: str = ""):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_replica_commands_stream(self, commands: List[Dict]) -> List[Dict]:
        """Process replica commands into raw transaction format for ingestion stream."""
        transactions = []

        for cmd in commands:
            for cmd in commands:
                if not isinstance(cmd, dict):
                    locals.error("Invalid command format", command=cmd)
                continue
            abci_block = cmd.get("abci_block", {})
            block_time = abci_block.get("time")
            block_number = abci_block.get("round")
            signed_bundles = abci_block.get("signed_action_bundles", [])
            full_response = cmd.get("resps", {}).get("Full", [])

            for n, bundle in enumerate(signed_bundles):
                block_hash = bundle[0] if len(bundle) > 0 else None
                bundle_data = bundle[1] if len(bundle) > 1 else {}

                response_pair = full_response[n] if n < len(full_response) else None
                response_hash = response_pair[0] if response_pair else None
                response_data = response_pair[1] if response_pair and len(response_pair) > 1 else []

                signed_actions = bundle_data.get("signed_actions", [])

                for i, action in enumerate(signed_actions):
                    action_data = action.get("action", {})

                    sig = action.get("signature", {})
                    r, s, v = sig.get("r"), sig.get("s"), sig.get("v")
                    final_sig = None
                    if r and s and v is not None:
                        try:
                            r = r[2:] if r.startswith("0x") else r
                            s = s[2:] if s.startswith("0x") else s
                            r, s = r.zfill(64), s.zfill(64)
                            v = int(v) + 27 if int(v) in (0, 1) else int(v)
                            final_sig = rsv_to_signature(r, s, v)
                        except Exception as e:
                            logger.warning(f"Failed to format signature: {e}")

                    user = action.get("user") or action_data.get("user")
                    wallet_address = user
                    wallet_type = "USER" if user else None
                    if not wallet_address and action.get("vaultAddress"):
                        wallet_address = action.get("vaultAddress")
                        wallet_type = "VAULT"

                    if not wallet_address and block_hash == response_hash:
                        if i < len(response_data):
                            wallet_address = response_data[i].get("user")
                            wallet_type = "USER"

                    action_nonce = action.get("nonce") or action_data.get("nonce")
                    exchange = "Hyperliquid" if action.get('isFrontend') == True else None
                    contract_address = action.get("contractAddress")
                    gas_price = action.get("gasPrice")
                    error_message = action.get("error")

                    method = action_data.get("type")
                    order_grouping = action_data.get("grouping")
                    input_data = safe_json_dumps(action_data) if action_data else '{}'
                    chain_id = 999
                    now = datetime.utcnow().isoformat()
                    common_fields = {
                        "block_timestamp": to_iso(block_time),
                        "block_number": block_number,
                        "block_hash": block_hash,
                        "transaction_index": i,
                        "transaction_hash": action.get("raw_tx_hash") or None,
                        "wallet_address": wallet_address,
                        "wallet_type": wallet_type,
                        "nonce": action_nonce,
                        "signature": final_sig,
                        "gas_price": gas_price,
                        "method": method,
                        "order_grouping": order_grouping,
                        "chain_id": chain_id,
                        "exchange": exchange,
                        "input_data": [],
                        "agent_info": action.get("agent"),
                        "agent_address": action.get("agentAddress"),
                        "error_message": error_message,
                        "created_at": to_iso(now),
                        "updated_at": to_iso(now)
                    }

                    if method == "order":
                        for o in action_data.get("orders", []):
                            order_type = o.get("t", {})
                            time_in_force = None
                            if "limit" in order_type:
                                time_in_force = order_type["limit"].get("tif")
                            elif "trigger" in order_type:
                                time_in_force = order_type["trigger"].get("tif")

                            recorded_keys = {"a", "b", "p", "s", "r", "t", "tif", "c"}
                            extra_keys = set(o.keys()) - recorded_keys

                            txn = {
                                "id": str(uuid.uuid4()),
                                "contract_address": contract_address,
                                "asset_id": o.get("a"),
                                "order_id": o.get("c") if  o.get("c") else None,
                                "is_buy": o.get("b"),
                                "price": o.get("p"),
                                "size": o.get("s"),
                                "reduce_only": o.get("r"),
                                "time_in_force": time_in_force,
                                **common_fields
                            }

                            if extra_keys:
                                txn["input_data"] = input_data
                            transactions.append(txn)

                    elif method == "cancel":
                        for cancel in action_data.get("cancels", []):
                            recorded_keys = {"a", "o"}
                            extra_keys = set(cancel.keys()) - recorded_keys
                            txn = {
                                "id": str(uuid.uuid4()),
                                "contract_address": contract_address,
                                "asset_id": cancel.get("a"),
                                "order_id": cancel.get("o"),
                                "price": None,
                                "size": None,
                                "is_buy": None,
                                "reduce_only": None,
                                "time_in_force": None,
                                **common_fields
                            }

                            if extra_keys:
                                txn["input_data"] = input_data
                            transactions.append(txn)

                    elif method == "cancelByCloid":
                        for cancel in action_data.get("cancels", []):
                            recorded_keys = {"cloid", "asset"}
                            extra_keys = set(cancel.keys()) - recorded_keys
                            txn = {
                                "id": str(uuid.uuid4()),
                                "contract_address": contract_address,
                                "asset_id": cancel.get("asset"),
                                "order_id": cancel.get("cloid"),
                                "price": None,
                                "size": None,
                                "is_buy": None,
                                "reduce_only": None,
                                "time_in_force": None,
                                **common_fields
                            }
                            if extra_keys:
                                txn["input_data"] = input_data

                            transactions.append(txn)
                            
                    elif method == "bridge":
                        recorded_keys = {"ethTxHash"}
                        extra_keys = set(cancel.keys()) - recorded_keys
                        txn = ({
                            "id": str(uuid.uuid4()),
                            "contract_address": action_data.get("ethTxHash"),
                            "asset_id": None,
                            "order_id": None,
                            "price": None,
                            "size": action.get("amount"),
                            "is_buy": None,
                            "reduce_only": None,
                            "time_in_force": None,
                            **common_fields
                        })

                        if extra_keys:
                                txn["input_data"] = input_data

                        transactions.append(txn)

                    elif method == "connect":
                        transactions.append({
                            "id": str(uuid.uuid4()),
                            "contract_address": contract_address,
                            "input_data": input_data,
                            "asset_id": None,
                            "order_id": None,
                            "price": None,
                            "size": None,
                            "is_buy": None,
                            "reduce_only": None,
                            "time_in_force": None,
                            **common_fields
                        })

                    else:
                        # fallback if no specific method handler
                        transactions.append({
                            "id": str(uuid.uuid4()),
                            "contract_address": contract_address,
                            "input_data": input_data,
                            "asset_id": None,
                            "order_id": None,
                            "price": None,
                            "size": None,
                            "is_buy": None,
                            "reduce_only": None,
                            "time_in_force": None,
                            **common_fields
                        })

        logger.info(f"Parsed {len(transactions)} transactions from {len(commands)} command lines.")
        return transactions


def main():
    """Main extraction workflow for replica commands and explorer blocks."""
    pass

if __name__ == "__main__":
    main() 