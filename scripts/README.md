# Hyperliquid Data Pipeline Scripts

This directory contains the modularized scripts for the Hyperliquid data processing pipeline. The scripts have been organized into a clean, modular structure for better maintainability and testing.

## Directory Structure

```
scripts/
├── main.py                    # Main entry point for all operations
├── core/                      # Core data processing modules
│   ├── __init__.py
│   ├── signature_utils.py     # ECDSA signature conversion utilities
│   ├── extractors/            # Data extraction modules
│   │   ├── __init__.py
│   │   ├── replica_commands.py     # Replica commands extractor
│   │   └── backfill_processor.py   # Historical backfill processor
│   └── database/              # Database management utilities
│       ├── __init__.py
│       └── partition_manager.py    # Time-series partition management
├── deployment/                # Deployment and infrastructure
│   ├── deploy_schema.sh      # Database schema deployment
│   └── deploy_cron.sh        # Cron job deployment
├── monitoring/                # Monitoring and health checks
│   └── health_check.sh       # System health monitoring
└── tests/                     # Test suite
    ├── __init__.py
    └── test_extraction.py     # Extraction tests
```

## Quick Start

### Using the Main Interface

The `main.py` script provides a unified command-line interface:

```bash
# Run the test suite
python main.py test

# Check pipeline status
python main.py status

# Extract replica commands data
python main.py extract-replica data/replica_cmds/file.lz4 --max-lines 1000

# Run incremental backfill
python main.py backfill --period incremental --max-files 50

# Manage database partitions
python main.py partitions --action create
```

### Direct Module Usage

You can also import and use modules directly:

```python
from core.signature_utils import rsv_to_signature
from core.extractors.replica_commands import ReplicaCommandsExtractor
from core.extractors.backfill_processor import HyperliquidBackfillProcessor
```

## Core Modules

### Signature Utils (`core/signature_utils.py`)

Handles ECDSA signature conversion between formats:

- **`rsv_to_signature(r, s, v)`**: Convert separate components to standard 65-byte format
- **`signature_to_rsv(signature)`**: Convert back to separate components  
- **`validate_signature_format(signature)`**: Validate signature format

**Example:**

```python
from core.signature_utils import rsv_to_signature

# Convert components to standard format
signature = rsv_to_signature(
    '0x689057082784b47a31d68a5a6697227c04fc2eff7b02bf71b792ce9f5d8ead02',
    '0x24ac23ad9cabc41d9197f689042f01c152506bf6b943995afd645d6bea52a93a',
    28
)
# Result: 130-character concatenated signature
```

### Replica Commands Extractor (`core/extractors/replica_commands.py`)

Extracts and processes Hyperliquid replica commands with signature conversion:

- **`extract_replica_commands(filepath, max_lines)`**: Extract raw commands from LZ4 files
- **`process_replica_commands_for_db(commands)`**: Convert to database format with signature conversion
- **`generate_sql_inserts(transactions)`**: Generate SQL INSERT statements

**Features:**

- Automatic signature format conversion (r,s,v to concatenated)
- Action field extraction (orders, cancels, etc.)
- EVM-compatible field naming
- Full nonce handling

### Backfill Processor (`core/extractors/backfill_processor.py`)

Historical data backfill with state management:

- **Period-based processing**: Different strategies for different time periods
- **Gap detection**: Automatic identification of missing data ranges
- **Deduplication**: Prevents duplicate processing across sources
- **State persistence**: Resume capability for interrupted processing

**Processing Periods:**

- **Pre-March 2025**: Explorer blocks only
- **March-May 2025**: Explorer blocks + node_trades validation  
- **Post-May 2025**: Explorer blocks + node_fills complete data

### Partition Manager (`core/database/partition_manager.py`)

Manages time-series partitions for optimal query performance:

- **Automatic partition creation**: Future partitions for continuous operation
- **Cleanup management**: Remove old partitions based on retention policy
- **Status monitoring**: Track partition health and usage

## Testing

### Test Suite (`tests/test_extraction.py`)

Test coverage including:

- **Signature conversion tests**: Format validation and round-trip testing
- **Extraction logic tests**: Data processing and field mapping validation
- **Integration tests**: End-to-end pipeline testing with real data structures
- **Error handling tests**: Edge cases and invalid input handling

**Run tests:**

```bash
# Run all tests
python main.py test

# Run tests directly
python tests/test_extraction.py
```

### Test Coverage

- **Signature Utils**: 100% coverage of conversion functions
- **Replica Extractor**: Action processing, field mapping, signature conversion
- **Backfill Processor**: Nonce extraction, action field parsing
- **Integration**: End-to-end data flow validation

## Key Improvements

### 1. **Modular Architecture**

- **Separation of concerns**: Each module has a specific responsibility
- **Clean interfaces**: Well-defined APIs between components
- **Easy testing**: Isolated components for targeted testing

### 2. **Standard ECDSA Signatures**

- **Ethereum compatibility**: Uses standard 65-byte concatenated format
- **Tool compatibility**: Works with standard blockchain analysis tools
- **Storage efficiency**: Single field vs. separate r, s, v fields

### 3. **EVM Field Naming**

- **Industry standard**: Uses standard blockchain field names
- **Tool compatibility**: Integrates with existing blockchain tools
- **Clarity**: Clear, descriptive field names

### 4. **Full Testing**

- **Automated validation**: Complete test suite for all components
- **Real data testing**: Tests against actual Hyperliquid data structures
- **Error handling**: Robust error case coverage

## Deployment Scripts

### Schema Deployment (`deployment/deploy_schema.sh`)

Deploys database schemas with proper error handling and validation.

### Cron Deployment (`deployment/deploy_cron.sh`)  

Sets up automated data processing jobs with proper scheduling.

### Health Monitoring (`monitoring/health_check.sh`)

Monitors pipeline health and alerts on issues.

## Usage Examples

### Extract Replica Commands

```bash
python main.py extract-replica data/replica_cmds/476360000.lz4 \
  --max-lines 5000 \
  --output-dir processed_data
```

### Run Backfill

```bash
# Incremental backfill (default)
python main.py backfill --period incremental --max-files 100

# Full historical backfill
python main.py backfill --period full --config configs/production_config.yaml
```

### Database Maintenance

```bash
# Create future partitions
python main.py partitions --action create

# Check partition status
python main.py partitions --action status

# Cleanup old partitions
python main.py partitions --action cleanup
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you're running from the scripts directory
2. **Missing Dependencies**: Run `pip install -r ../requirements.txt`
3. **Configuration Issues**: Verify config file paths and database credentials

### Debug Mode

```bash
# Enable verbose logging
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python -v main.py status
```

## Performance

- **Modular design**: Reduced memory footprint per operation
- **Standard signatures**: ~33% storage reduction (3 fields to 1 field)
- **Batch processing**: Efficient database operations
- **State management**: Resume capability reduces reprocessing
