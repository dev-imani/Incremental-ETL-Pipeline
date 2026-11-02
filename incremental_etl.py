
# """
# Day 3 - Incremental ETL Pipeline
# Squad Python Specialization - Data Engineering

# Run twice in a row. The second run must change nothing.
# """

# import csv
# import json
# import logging
# import math
# import os
# import tempfile
# from datetime import datetime, timezone
# from pathlib import Path

# WAREHOUSE_PATH = Path("data") / "transactions.csv"
# BATCH_DIR = Path("incoming")
# CHECKPOINT_PATH = Path("state") / "checkpoint.json"
# LOG_PATH = Path("logs") / "etl.log"

# KEY = "transaction_id"
# COMPARED_FIELDS = (
#     "customer_id",
#     "merchant",
#     "amount_kes",
#     "status",
#     "channel",
#     "updated_at",
# )
# CSV_FIELDS = (KEY,) + COMPARED_FIELDS
# REQUIRED_FIELDS = CSV_FIELDS


# def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
#     """Configure logging to both the console and a file."""
#     path.parent.mkdir(parents=True, exist_ok=True)

#     logger = logging.getLogger("lipa_haraka_etl")
#     logger.setLevel(logging.INFO)
#     logger.propagate = False

#     # Avoid adding duplicate handlers if run() is called more than once.
#     if not logger.handlers:
#         formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s" )

#         console = logging.StreamHandler()
#         console.setFormatter(formatter)

#         file_handler = logging.FileHandler(path, encoding="utf-8")
#         file_handler.setFormatter(formatter)

#         logger.addHandler(console)
#         logger.addHandler(file_handler)

#     return logger


# def parse_timestamp(value: str) -> datetime:
#     """Parse an ISO-8601 timestamp, including timestamps ending in Z."""
#     parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))

#     # Treat timestamps without an offset as UTC.
#     if parsed.tzinfo is None:
#         parsed = parsed.replace(tzinfo=timezone.utc)

#     return parsed.astimezone(timezone.utc)


# def normalise_amount(value) -> float:
#     """Convert an amount to a finite, non-negative number."""
#     if isinstance(value, bool):
#         raise ValueError("amount_kes must be numeric")

#     amount = float(value)

#     if not math.isfinite(amount) or amount < 0:
#         raise ValueError("amount_kes must be finite and non-negative")

#     return amount


# def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
#     """Read the loaded transactions into a dictionary keyed by transaction ID."""
#     if not path.exists():
#         return {}

#     state = {}

#     with path.open("r", newline="", encoding="utf-8-sig") as file:
#         reader = csv.DictReader(file)

#         if not reader.fieldnames or KEY not in reader.fieldnames:
#             raise ValueError(f"{path} is missing the {KEY} column")

#         for row in reader:
#             transaction_id = (row.get(KEY) or "").strip()

#             if not transaction_id:
#                 raise ValueError("Existing CSV contains an empty transaction_id")

#             if transaction_id in state:
#                 raise ValueError(
#                     f"Existing CSV contains duplicate ID: {transaction_id}"
#                 )

#             record = {field: row.get(field, "") for field in CSV_FIELDS}
#             record[KEY] = transaction_id
#             record["amount_kes"] = normalise_amount( record["amount_kes"] )

#             state[transaction_id] = record

#     return state


# def load_batch(path: Path) -> tuple[str, list[dict]]:
#     """Read a JSON batch and return its batch ID and transactions."""
#     with path.open("r", encoding="utf-8") as file:
#         payload = json.load(file)

#     if not isinstance(payload, dict):
#         raise ValueError("Batch JSON must contain an object")

#     batch_id = payload.get("batch_id")
#     transactions = payload.get("transactions")

#     if not isinstance(batch_id, str) or not batch_id.strip():
#         raise ValueError("Missing or invalid batch_id")

#     if not isinstance(transactions, list):
#         raise ValueError("Batch transactions must be a list")

#     return batch_id.strip(), transactions


# def validate_record(record: dict) -> list[str]:
#     """Return validation errors for a transaction."""
#     if not isinstance(record, dict):
#         return ["record must be a JSON object"]

#     reasons = []

#     for field in REQUIRED_FIELDS:
#         value = record.get(field)

#         if value is None or (
#             isinstance(value, str) and not value.strip()
#         ):
#             reasons.append(f"missing required field: {field}")

#     if reasons:
#         return reasons

#     try:
#         normalise_amount(record["amount_kes"])
#     except (ValueError, TypeError, OverflowError):
#         reasons.append("amount_kes must be a finite, non-negative number")

#     try:
#         parse_timestamp(record["updated_at"])
#     except (ValueError, TypeError, OverflowError):
#         reasons.append("updated_at must be a valid ISO-8601 timestamp")

#     return reasons


# def prepare_record(record: dict) -> dict:
#     """Return a validated record in a consistent representation."""
#     prepared = {field: record[field] for field in CSV_FIELDS}
#     prepared[KEY] = str(prepared[KEY]).strip()
#     prepared["amount_kes"] = normalise_amount(
#         prepared["amount_kes"]
#     )
#     prepared["updated_at"] = parse_timestamp(prepared["updated_at"]).isoformat()

#     for field in ("customer_id", "merchant", "status", "channel"):
#         prepared[field] = str(prepared[field]).strip()

#     return prepared


# def record_signature(record: dict) -> str:
#     """Create a stable representation for deterministic tie-breaking."""
#     return json.dumps({field: record[field] for field in CSV_FIELDS},sort_keys=True,default=str, )


# def classify(existing: dict[str, dict],batch: list[dict],) -> dict[str, list[dict]]:
#     """Classify incoming transactions into new, updated, duplicate and invalid."""
#     buckets = {
#         "new": [],
#         "updated": [],
#         "duplicate": [],
#         "invalid": [],
#     }

#     # Validate records first and group valid records by transaction ID.
#     groups = {}

#     for record in batch:
#         reasons = validate_record(record)

#         if reasons:
#             buckets["invalid"].append(record)
#             continue

#         prepared = prepare_record(record)
#         transaction_id = prepared[KEY]
#         groups.setdefault(transaction_id, []).append(prepared)

#     existing_ids = set(existing)
#     incoming_ids = set(groups)

#     # Set operations identify unseen and overlapping transaction IDs.
#     new_ids = incoming_ids - existing_ids
#     overlapping_ids = incoming_ids & existing_ids

#     def latest_version(records):
#         """Choose the newest timestamp, with a stable tie-breaker."""
#         return sorted(
#             records,
#             key=lambda r: (
#                 parse_timestamp(r["updated_at"]),
#                 record_signature(r),
#             ),
#         )[-1]

#     for transaction_id in sorted(new_ids):
#         versions = groups[transaction_id]
#         winner = latest_version(versions)
#         buckets["new"].append(winner)

#         # Additional records with the same ID are not inserted twice.
#         for record in versions:
#             if record is not winner:
#                 buckets["duplicate"].append(record)

#     for transaction_id in sorted(overlapping_ids):
#         versions = groups[transaction_id]
#         winner = latest_version(versions)

#         stored = existing[transaction_id]
#         incoming_time = parse_timestamp(winner["updated_at"])
#         stored_time = parse_timestamp(stored["updated_at"])

#         stored_amount = normalise_amount(stored["amount_kes"])
#         incoming_amount = normalise_amount(winner["amount_kes"])

#         same_values = all(
#             (
#                 stored_amount == incoming_amount
#                 if field == "amount_kes"
#                 else str(stored[field]) == str(winner[field])
#             )
#             for field in COMPARED_FIELDS
#         )

#         if same_values or incoming_time <= stored_time:
#             # An exact repeat or a stale version must not overwrite the row.
#             buckets["duplicate"].append(winner)
#         else:
#             buckets["updated"].append(winner)

#         for record in versions:
#             if record is not winner:
#                 buckets["duplicate"].append(record)

#     return buckets


# def upsert(
#     existing: dict[str, dict],
#     new: list[dict],
#     updated: list[dict],
# ) -> dict[str, dict]:
#     """Return a merged state without mutating the original dictionary."""
#     merged = dict(existing)

#     for record in new:
#         merged[record[KEY]] = record.copy()

#     for record in updated:
#         merged[record[KEY]] = record.copy()

#     return merged


# def atomic_write(path: Path, writer) -> None:
#     """Write to a temporary file in the same directory, then replace atomically."""
#     path.parent.mkdir(parents=True, exist_ok=True)
#     temp_path = None

#     try:
#         with tempfile.NamedTemporaryFile(
#             mode="w",
#             newline="",
#             encoding="utf-8",
#             dir=path.parent,
#             prefix=f".{path.name}.",
#             suffix=".tmp",
#             delete=False,
#         ) as file:
#             temp_path = Path(file.name)
#             writer(file)
#             file.flush()
#             os.fsync(file.fileno())

#         os.replace(temp_path, path)
#         temp_path = None

#     finally:
#         if temp_path is not None:
#             try:
#                 temp_path.unlink(missing_ok=True)
#             except OSError:
#                 pass


# def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
#     """Return the checkpoint, or a fresh checkpoint if none exists."""
#     if not path.exists():
#         return {"processed_batches": []}

#     with path.open("r", encoding="utf-8") as file:
#         checkpoint = json.load(file)

#     if (
#         not isinstance(checkpoint, dict)
#         or not isinstance(checkpoint.get("processed_batches"), list)
#     ):
#         raise ValueError("Invalid checkpoint structure")

#     return checkpoint


# def write_checkpoint(
#     checkpoint: dict,
#     path: Path = CHECKPOINT_PATH,
# ) -> None:
#     """Persist the checkpoint using an atomic file replacement."""
#     atomic_write(
#         path,
#         lambda file: json.dump(
#             checkpoint, file, indent=2, sort_keys=True
#         ),
#     )


# def save_warehouse(
#     state: dict[str, dict],
#     path: Path = WAREHOUSE_PATH,
# ) -> None:
#     """Save transactions in stable ID order using an atomic replacement."""
#     def write_csv(file):
#         writer = csv.DictWriter(file,fieldnames=CSV_FIELDS,extrasaction="ignore",)
#         writer.writeheader()

#         for transaction_id in sorted(state):
#             record = state[transaction_id]
#             writer.writerow({
#                 field: (
#                     normalise_amount(record[field])
#                     if field == "amount_kes"
#                     else record[field]
#                 )
#                 for field in CSV_FIELDS
#             })

#     atomic_write(path, write_csv)


# def run() -> dict:
#     """Process batches not yet recorded in the checkpoint."""
#     logger = setup_logging()

#     # Do not mark a batch as processed if the warehouse cannot be read.
#     try:
#         existing = load_existing()
#         checkpoint = read_checkpoint()
#     except (OSError, csv.Error, json.JSONDecodeError, ValueError) as exc:
#         logger.exception("Could not load warehouse or checkpoint: %s", exc)
#         return {"status": "error", "error": str(exc)}

#     processed = set(checkpoint["processed_batches"])
#     totals = {"new": 0,"updated": 0,"duplicate": 0,"invalid": 0,"skipped_batches": 0,"processed_batches": 0,}

#     BATCH_DIR.mkdir(parents=True, exist_ok=True)

#     for path in sorted(BATCH_DIR.glob("*.json")):
#         try:
#             batch_id, transactions = load_batch(path)
#         except (OSError, json.JSONDecodeError, ValueError) as exc:
#             logger.warning(
#                 "Invalid batch file=%s reason=%s",
#                 path.name,
#                 exc,
#             )
#             totals["skipped_batches"] += 1
#             continue

#         if batch_id in processed:
#             logger.info(
#                 "Batch skipped batch_id=%s reason=already_processed",
#                 batch_id,
#             )
#             totals["skipped_batches"] += 1
#             continue

#         try:
#             buckets = classify(existing, transactions)

#             for record in buckets["invalid"]:
#                 logger.warning("Invalid record batch_id=%s transaction_id=%s reasons=%s",batch_id,record.get(KEY, "<missing>")
#                     if isinstance(record, dict)
#                     else "<not-an-object>",
#                     "; ".join(validate_record(record)),
#                 )

#             merged = upsert(existing,buckets["new"], buckets["updated"],)

#             # Save data FIRST. Only then record the batch as completed.
#             save_warehouse(merged)

#             updated_checkpoint = dict(checkpoint)
#             updated_checkpoint["processed_batches"] = (list(checkpoint["processed_batches"]) + [batch_id] )
#             write_checkpoint(updated_checkpoint)

#             # Update in-memory state only after both writes succeed.
#             existing = merged
#             checkpoint = updated_checkpoint
#             processed.add(batch_id)

#             for bucket in ("new", "updated", "duplicate", "invalid"):
#                 totals[bucket] += len(buckets[bucket])

#             totals["processed_batches"] += 1

#             logger.info("Batch complete batch_id=%s new=%d updated=%d " "duplicate=%d invalid=%d total_rows=%d",batch_id,
#                 len(buckets["new"]),
#                 len(buckets["updated"]),
#                 len(buckets["duplicate"]),
#                 len(buckets["invalid"]),
#                 len(existing),
#             )

#         except (OSError, csv.Error, ValueError, TypeError) as exc:
#             logger.exception(
#                 "Batch failed batch_id=%s error=%s",
#                 batch_id,
#                 exc,
#             )
#             # Leave the batch out of the checkpoint so it can be retried.

#     logger.info(
#         "Run complete batches=%d skipped_batches=%d new=%d updated=%d "
#         "duplicate=%d invalid=%d total_rows=%d",
#         totals["processed_batches"],
#         totals["skipped_batches"],
#         totals["new"],
#         totals["updated"],
#         totals["duplicate"],
#         totals["invalid"],
#         len(existing),
#     )

#     totals["total_rows"] = len(existing)
#     return totals


# if __name__ == "__main__":
#     run()

"""
Day 3 - Incremental ETL Pipeline
Squad Python Specialization - Data Engineering

Run it twice in a row. The second run must change nothing.
"""

import csv
import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path

WAREHOUSE_PATH = Path("data") / "transactions.csv"
BATCH_DIR = Path("incoming")
CHECKPOINT_PATH = Path("state") / "checkpoint.json"
LOG_PATH = Path("logs") / "etl.log"

KEY = "transaction_id"

COMPARED_FIELDS = (
    "customer_id",
    "merchant",
    "amount_kes",
    "status",
    "channel",
    "updated_at",
)

CSV_FIELDS = (KEY,) + COMPARED_FIELDS

BUCKETS = ("new", "updated", "duplicate", "invalid")


# --------------------------------------------------
# 1. SETUP LOGGING
# --------------------------------------------------

def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
    """Configure a logger that writes to both console and log file."""

    path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("incremental_etl")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Avoid adding duplicate handlers if run() is called repeatedly.
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# --------------------------------------------------
# 2. LOAD EXISTING TRANSACTIONS
# --------------------------------------------------

def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
    """Read existing transactions into a dictionary keyed by transaction_id."""

    logger = logging.getLogger("incremental_etl")

    if not path.exists():
        logger.warning("Warehouse file does not exist: %s", path)
        return {}

    existing = {}

    with path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            transaction_id = str(row.get(KEY, "")).strip()

            if not transaction_id:
                logger.warning("Skipping existing row without transaction_id")
                continue

            try:
                amount = float(row["amount_kes"])

                if not math.isfinite(amount):
                    raise ValueError("amount_kes must be finite")

                row["amount_kes"] = amount

            except (ValueError, TypeError, KeyError):
                logger.warning(
                    "Skipping existing transaction %s: invalid amount_kes",
                    transaction_id,
                )
                continue

            existing[transaction_id] = row

    logger.info("Loaded %d existing transactions", len(existing))

    return existing


# --------------------------------------------------
# 3. LOAD JSON BATCH
# --------------------------------------------------

def load_batch(path: Path) -> tuple[str, list[dict]]:
    """Read one JSON batch and return its batch_id and transactions."""

    logger = logging.getLogger("incremental_etl")

    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if not isinstance(data, dict):
            raise ValueError("Batch must contain a JSON object")

        batch_id = data.get("batch_id")
        transactions = data.get("transactions")

        if not isinstance(batch_id, str) or not batch_id.strip():
            raise ValueError("Missing or invalid batch_id")

        if not isinstance(transactions, list):
            raise ValueError("transactions must be a list")

        return batch_id.strip(), transactions

    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError) as error:
        logger.error("Could not load batch %s: %s", path, error)
        return "", []


# --------------------------------------------------
# 4. VALIDATE RECORDS
# --------------------------------------------------

def validate_record(record: dict) -> list[str]:
    """Return validation errors. An empty list means the record is valid."""

    errors = []

    if not isinstance(record, dict):
        return ["Record must be a dictionary"]

    # Check required fields.
    required_fields = (KEY,) + COMPARED_FIELDS

    for field in required_fields:
        value = record.get(field)

        if value is None or (isinstance(value, str) and not value.strip()):
            errors.append(f"Missing required field: {field}")

    # Validate transaction amount.
    try:
        amount = float(record["amount_kes"])

        if not math.isfinite(amount):
            errors.append("amount_kes must be a finite number")

        elif amount < 0:
            errors.append("amount_kes cannot be negative")

    except (ValueError, TypeError, KeyError):
        errors.append("amount_kes must be a valid number")

    # Validate timestamp.
    try:
        datetime.fromisoformat(
            str(record["updated_at"]).strip().replace("Z", "+00:00")
        )
    except (ValueError, TypeError, KeyError):
        errors.append("updated_at must be a valid ISO 8601 timestamp")

    return errors


# --------------------------------------------------
# 5. CLASSIFY RECORDS
# --------------------------------------------------

def classify(
    existing: dict[str, dict],
    batch: list[dict]
) -> dict[str, list[dict]]:
    """
    Classify incoming records into:
    new, updated, duplicate and invalid.

    Only newer records are allowed to replace existing records.
    """

    buckets = {
        "new": [],
        "updated": [],
        "duplicate": [],
        "invalid": [],
    }

    # Temporary state helps us detect repeated IDs within the same batch.
    working = {
        transaction_id: row.copy()
        for transaction_id, row in existing.items()
    }

    # Use sets to compare transaction IDs.
    existing_ids = set(existing.keys())

    valid_records = []
    seen_batch_ids = set()

    for record in batch:

        errors = validate_record(record)

        if errors:
            buckets["invalid"].append({
                "record": record,
                "errors": errors,
            })
            continue

        # Copy the record to avoid modifying the original JSON data.
        incoming = record.copy()

        incoming[KEY] = str(incoming[KEY]).strip()
        incoming["amount_kes"] = float(incoming["amount_kes"])

        valid_records.append(incoming)

    batch_ids = {record[KEY] for record in valid_records}

    unseen_ids = batch_ids - existing_ids
    overlapping_ids = batch_ids & existing_ids

    # Process records in their original batch order.
    for incoming in valid_records:

        transaction_id = incoming[KEY]

        if transaction_id in seen_batch_ids:
            previous = working[transaction_id]

            if all(
                incoming[field] == previous[field]
                for field in COMPARED_FIELDS
            ):
                buckets["duplicate"].append(incoming)
                continue

            incoming_time = datetime.fromisoformat(
                incoming["updated_at"].replace("Z", "+00:00")
            )

            previous_time = datetime.fromisoformat(
                previous["updated_at"].replace("Z", "+00:00")
            )

            # A repeated ID with an older or equal timestamp
            # cannot replace the latest record.
            if incoming_time <= previous_time:
                buckets["duplicate"].append(incoming)
                continue

            buckets["updated"].append(incoming)
            working[transaction_id] = incoming
            continue

        seen_batch_ids.add(transaction_id)

        if transaction_id in unseen_ids:

            buckets["new"].append(incoming)
            working[transaction_id] = incoming

        elif transaction_id in overlapping_ids:

            stored = working[transaction_id]

            identical = all(
                incoming[field] == stored[field]
                for field in COMPARED_FIELDS
            )

            if identical:
                buckets["duplicate"].append(incoming)
                continue

            incoming_time = datetime.fromisoformat(
                incoming["updated_at"].replace("Z", "+00:00")
            )

            stored_time = datetime.fromisoformat(
                stored["updated_at"].replace("Z", "+00:00")
            )

            if incoming_time > stored_time:
                buckets["updated"].append(incoming)
                working[transaction_id] = incoming

            else:
                # Older or equal conflicting records are not applied.
                buckets["duplicate"].append(incoming)

    return buckets


# --------------------------------------------------
# 6. UPSERT RECORDS
# --------------------------------------------------

def upsert(
    existing: dict[str, dict],
    new: list[dict],
    updated: list[dict]
) -> dict[str, dict]:
    """Insert new transactions and replace updated transactions."""

    state = {
        transaction_id: row.copy()
        for transaction_id, row in existing.items()
    }

    for record in new:
        state[record[KEY]] = record.copy()

    for record in updated:
        state[record[KEY]] = record.copy()

    return state


# --------------------------------------------------
# 7. CHECKPOINT MANAGEMENT
# --------------------------------------------------

def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
    """Read checkpoint or return a fresh checkpoint."""

    logger = logging.getLogger("incremental_etl")

    if not path.exists():
        return {"processed_batches": []}

    try:
        with path.open("r", encoding="utf-8") as file:
            checkpoint = json.load(file)

        if (
            not isinstance(checkpoint, dict)
            or not isinstance(checkpoint.get("processed_batches"), list)
            or not all(
                isinstance(batch_id, str)
                for batch_id in checkpoint["processed_batches"]
            )
        ):
            raise ValueError("Invalid checkpoint structure")

        return checkpoint

    except (json.JSONDecodeError, OSError, ValueError) as error:
        logger.error("Could not read checkpoint: %s", error)
        raise


def write_checkpoint(
    checkpoint: dict,
    path: Path = CHECKPOINT_PATH
) -> None:
    """Write checkpoint atomically using a temporary file."""

    path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = path.with_name(path.name + ".tmp")

    try:
        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(checkpoint, file, indent=4)
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_path, path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


# --------------------------------------------------
# 8. SAVE WAREHOUSE
# --------------------------------------------------

def save_warehouse(
    state: dict[str, dict],
    path: Path = WAREHOUSE_PATH
) -> None:
    """Save transactions to CSV atomically in a stable order."""

    path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = path.with_name(path.name + ".tmp")

    try:
        with temporary_path.open(
            "w",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.DictWriter(
                file,
                fieldnames=CSV_FIELDS
            )

            writer.writeheader()

            for transaction_id in sorted(state.keys()):
                row = state[transaction_id]

                writer.writerow({
                    field: row.get(field, "")
                    for field in CSV_FIELDS
                })

            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_path, path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


# --------------------------------------------------
# 9. RUN PIPELINE
# --------------------------------------------------

def run() -> dict:
    """Process every batch not yet recorded in the checkpoint."""

    logger = setup_logging()

    logger.info("=" * 50)
    logger.info("Starting incremental ETL pipeline")

    existing = load_existing()
    checkpoint = read_checkpoint()

    processed_batches = set(checkpoint["processed_batches"])

    statistics = {
        "batches_processed": 0,
        "batches_skipped": 0,
        "new": 0,
        "updated": 0,
        "duplicate": 0,
        "invalid": 0,
    }

    if not BATCH_DIR.exists():
        logger.warning("Incoming directory does not exist: %s", BATCH_DIR)
        return statistics

    batch_files = sorted(BATCH_DIR.glob("*.json"))

    for batch_path in batch_files:

        batch_id, transactions = load_batch(batch_path)

        # Skip malformed batches.
        if not batch_id:
            statistics["batches_skipped"] += 1
            continue

        # Skip batches already processed.
        if batch_id in processed_batches:
            logger.info("Skipping already processed batch: %s", batch_id)
            statistics["batches_skipped"] += 1
            continue

        logger.info("Processing batch: %s", batch_id)

        buckets = classify(existing, transactions)

        # Log one WARNING per invalid record.
        for invalid_record in buckets["invalid"]:
            logger.warning(
                "Invalid record %s: %s",
                invalid_record["record"].get(KEY, "UNKNOWN"),
                "; ".join(invalid_record["errors"]),
            )

        # Calculate the new warehouse state.
        updated_state = upsert(
            existing,
            buckets["new"],
            buckets["updated"],
        )

        # Save warehouse BEFORE updating the checkpoint.
        try:
            save_warehouse(updated_state)

            # Only mark the batch processed after the warehouse is saved.
            checkpoint["processed_batches"].append(batch_id)
            write_checkpoint(checkpoint)

        except OSError:
            logger.exception(
                "Failed to save batch %s. It will be retried.",
                batch_id,
            )
            raise

        # Update in-memory state for the next batch.
        existing = updated_state
        processed_batches.add(batch_id)

        # Record statistics.
        for bucket in BUCKETS:
            statistics[bucket] += len(buckets[bucket])

        statistics["batches_processed"] += 1

        logger.info(
            "Batch %s results | New: %d | Updated: %d | "
            "Duplicate: %d | Invalid: %d",
            batch_id,
            len(buckets["new"]),
            len(buckets["updated"]),
            len(buckets["duplicate"]),
            len(buckets["invalid"]),
        )

    logger.info("-" * 50)

    logger.info(
        "Pipeline completed | Batches processed: %d | "
        "Skipped: %d | New: %d | Updated: %d | "
        "Duplicate: %d | Invalid: %d",
        statistics["batches_processed"],
        statistics["batches_skipped"],
        statistics["new"],
        statistics["updated"],
        statistics["duplicate"],
        statistics["invalid"],
    )

    logger.info("Incremental ETL pipeline finished")

    return statistics


if __name__ == "__main__":
    run()
