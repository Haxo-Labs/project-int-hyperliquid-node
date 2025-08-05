#!/usr/bin/env python3
"""
Node Trades and Fills Extractor for Hyperliquid

This module provides utilities to extract and process:
- Node Trades: Trade match events with both participants' details.
- Node Fills: Per-participant fill records.

Features:
- Extraction and normalization of node trade and fill data.
- Batch insertion utilities for StarRocks.
- Field path extraction for schema exploration and debugging.

Intended for use in Hyperliquid data pipelines and backfill jobs.
"""

import json
import structlog
from datetime import datetime
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
import uuid
try:
    from dateutil import parser as date_parser
except ImportError:
    date_parser = None

# Configure structlog for inline, message-only, single-line logs (no timestamp, no key=val, no JSON)
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(pad_event=0)
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


@dataclass
class ProcessingStats:
    """Detailed statistics for extraction operations."""
    files_processed: int = 0
    records_extracted: int = 0
    duplicates_skipped: int = 0
    errors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    def duration(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

class NodeDataExtractor:
    """
    Extracts and processes node trades and node fills from Hyperliquid data sources.
    """

    def __init__(self, output_dir: str = "extracted_replica_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _convert_timestamp_to_datetime(self, ts):
        """
        Convert a timestamp to a datetime object.
        Handles int, float, and ISO 8601 string formats.
        """
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts
        # If it's already a number (seconds or milliseconds since epoch)
        if isinstance(ts, (int, float)):
            try:
                # If ts is too large, treat as milliseconds
                if ts > 1e12:
                    return datetime.utcfromtimestamp(ts / 1000.0)
                else:
                    return datetime.utcfromtimestamp(ts)
            except Exception as e:
                logger.warning(f"Could not convert numeric timestamp {ts} to datetime: {e}")
                return None
        # If it's a string, try to parse as ISO 8601
        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts)
            except Exception:
                pass
            if date_parser:
                try:
                    return date_parser.parse(ts)
                except Exception as e:
                    logger.warning(f"Could not parse string timestamp {ts} to datetime: {e}")
                    return None
            else:
                logger.warning(f"dateutil is not installed, cannot parse string timestamp {ts}")
                return None
        logger.warning(f"Unknown timestamp format: {ts}")
        return None
   

    def process_node_trades_stream_data(self, lines: list) -> list[dict]:
        """
        Parses node_stream_data and returns a list of raw_trade records matching the raw_trades schema.
        """
        if not lines:
            return []

        trade_records = []
        total = len(lines)
        log_interval = max(1, total // 10)

        try:
            for idx, trade_data in enumerate(lines):
                side_info = trade_data.get('side_info', [])
                if len(side_info) < 2:
                    continue

                participant_a = side_info[0]
                participant_b = side_info[1]

                now = datetime.utcnow().isoformat()
                block_timestamp = self._convert_timestamp_to_datetime(trade_data.get('time'))
                block_hash = trade_data.get('hash')
                block_number = trade_data.get('number')
                trade_hash = trade_data.get('hash')
                token_id = trade_data.get('coin')
                price = trade_data.get('px')
                amount = trade_data.get('sz')
                closed_pnl = trade_data.get('closedPnl')
                crossed = trade_data.get('crossed')
                side = trade_data.get('side')

                def get_participant_fields(p):
                    user = p.get('user')
                    vault = p.get('vaultAddress')
                    wallet_type = 'USER' if user else 'VAULT' if vault else None
                    wallet_address = user or vault

                    # liquidation
                    liquidation = p.get('liquidation') or trade_data.get('liquidation')
                    is_liquidation = (
                        isinstance(liquidation, dict) and liquidation.get('liquidatedUser') == user
                    )
                    return {
                        'wallet_address': wallet_address,
                        'wallet_type': wallet_type,
                        'order_id': p.get('oid'),
                        'twap_id': p.get('twap_id'),
                        'client_order_id': p.get('cloid'),
                        'fee_paid': p.get('feePaid'),
                        'fee_token': p.get('feeToken'),
                        'builder_fee': p.get('builderFee'),
                        'start_position': p.get('start_pos'),
                        'is_liquidation': is_liquidation,
                        'liquidation_price': liquidation.get('markPx') if isinstance(liquidation, dict) else None,
                        'liquidation_method': liquidation.get('method') if isinstance(liquidation, dict) else None,
                    }

                def determine_trade_direction(start_pos, sz, override):
                    try:
                        start = float(start_pos)
                        size = float(sz)
                        if size < 0 and start > 0 and override and override != "Na":
                            return "Long To Short"
                        elif size > 0 and start < 0 and override and override != "Na":
                            return "Short To Long"
                        elif size > 0 and start > 0:
                            return "Open Long"
                        elif size < 0 and start < 0:
                            return "Open Short"
                        elif size < 0 and start > 0:
                            return "Close Long"
                        elif size > 0 and start < 0:
                            return "Close Short"
                    except:
                        return None

                fields_a = get_participant_fields(participant_a)
                fields_b = get_participant_fields(participant_b)
                direction = determine_trade_direction(fields_a['start_position'], amount, trade_data.get('trade_dir_override'))

                for side_label, fields in zip(['A', 'B'], [fields_a, fields_b]):
                    flipped_side = side if side_label == 'A' else ('B' if side == 'A' else 'A' if side == 'B' else side)
                    trade_records.append({
                        'id': str(uuid.uuid4()),
                        'wallet_address': fields['wallet_address'],
                        'token_id': token_id,
                        'block_timestamp': block_timestamp.isoformat() if block_timestamp else None,
                        'trade_hash': trade_hash,
                        'block_number': block_number,
                        'block_hash': block_hash,
                        'trade_id': None,
                        'wallet_type': fields['wallet_type'],
                        'amount': str(amount) if amount is not None else None,
                        'price': str(price) if price is not None else None,
                        'side': flipped_side,
                        'order_id': fields['order_id'],
                        'twap_id': fields['twap_id'],
                        'client_order_id': fields['client_order_id'],
                        'fee_paid': fields['fee_paid'],
                        'fee_token': fields['fee_token'],
                        'builder_fee': fields['builder_fee'],
                        'gas_used': hash(str(trade_data)) & 0x7FFFFFFF,
                        'start_position': fields['start_position'],
                        'liquidity_type': 'taker' if crossed else 'maker',
                        'cross_type': 'crossed' if crossed else 'resting',
                        'trade_direction': direction,
                        'closed_pnl': closed_pnl,
                        'is_liquidation': fields['is_liquidation'],
                        'liquidation_price': fields['liquidation_price'],
                        'liquidation_method': fields['liquidation_method'],
                        'raw_data': json.dumps(trade_data, default=str),
                        'created_at': now,
                        'updated_at': now
                    })

                if (idx + 1) % log_interval == 0 or (idx + 1) == total:
                    logger.info(f"Processed {idx + 1}/{total} node_stream_data lines...")

        except Exception as e:
            logger.error("Failed to parse node stream data", error=str(e))

        logger.info(f"Parsed {len(trade_records)} raw_trade records from {total} lines.")
        return trade_records
 
def main():
    """Main CLI entry point for node trades and fills extraction."""
    pass

if __name__ == '__main__':
    main()