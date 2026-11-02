# Day 3 Incremental ETL

A small, dependency-free Python ETL pipeline for loading transaction batches from
JSON files into a CSV warehouse. The pipeline is incremental and idempotent:
already-processed batches are skipped, duplicate records are ignored, and only
newer versions of existing transactions are applied.

## Requirements

- Python 3.14 or newer

The project uses only Python standard-library modules at runtime. The
`requirements.txt` file is currently empty.

## Quick start

From the project directory:

```powershell
python incremental_etl.py
```

The command:

1. Loads the existing warehouse from `data/transactions.csv`.
2. Finds JSON files in `incoming/`.
3. Skips batches listed in `state/checkpoint.json`.
4. Validates and classifies incoming transactions.
5. Writes new and updated transactions to the warehouse.
6. Records successfully saved batch IDs in the checkpoint.
7. Writes progress and validation warnings to the console and `logs/etl.log`.

Run the command again to verify incremental behavior. A batch that has already
been checkpointed will be skipped without changing the warehouse.

## Directory structure

```text
.
├── incremental_etl.py                 # Main ETL pipeline
├── data/
│   └── transactions.csv               # CSV warehouse
├── incoming/
│   └── *.json                         # Input transaction batches
├── state/
│   └── checkpoint.json                # Successfully processed batch IDs
├── logs/
│   └── etl.log                        # Pipeline log output
├── src/day3_incremental_etl/
│   └── __init__.py                    # Package/CLI scaffold
├── l_1.py                             # Logging experiment
├── pyproject.toml                     # Project metadata
└── requirements.txt                   # Python dependencies
```

## Input format

Each file in `incoming/` must contain a JSON object with a `batch_id` and a
`transactions` list:

```json
{
  "batch_id": "BATCH-2026-10-21-01",
  "transactions": [
    {
      "transaction_id": "MP-50021",
      "customer_id": "CUST-145",
      "merchant": "Chandarana Kileleshwa",
      "amount_kes": 1780.0,
      "status": "COMPLETED",
      "channel": "M-Pesa",
      "updated_at": "2026-10-21T07:15:00"
    }
  ]
}
```

Required transaction fields are:

- `transaction_id`
- `customer_id`
- `merchant`
- `amount_kes`
- `status`
- `channel`
- `updated_at`

`amount_kes` must be a finite, non-negative number, and `updated_at` must be a
valid ISO 8601 timestamp. Invalid records are logged and excluded from the
warehouse; they do not prevent valid records in the same batch from being
processed.

## Incremental behavior

Transactions are keyed by `transaction_id`:

- **New** IDs are inserted.
- **Updated** records replace an existing record only when their
  `updated_at` timestamp is newer.
- **Duplicate** records are ignored when they match an existing record or are
  older/equal versions.
- **Invalid** records are logged with their validation errors and excluded.

The warehouse is written before the checkpoint is updated. This means a failed
warehouse write does not mark the batch as processed, allowing it to be retried.

## Resetting local state

To replay the sample batch from scratch, remove or edit the generated state and
warehouse files, then run the pipeline again:

```powershell
Remove-Item state/checkpoint.json
Remove-Item data/transactions.csv
python incremental_etl.py
```

Only reset these files when you intend to rebuild the local warehouse. The
incoming JSON files are treated as source data and are not modified.

## Development notes

The project metadata exposes a `day3-incremental-etl` console script, but the
current `src/day3_incremental_etl/__init__.py` contains only a scaffold
`main()` function. Use `python incremental_etl.py` for the implemented pipeline.