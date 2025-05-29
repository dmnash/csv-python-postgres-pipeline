# CSV to PostgreSQL Ingestion Engine

## Overview

The **Ingestion Engine** is a modular, schema-driven Python pipeline for validating and ingesting structured CSV data into a PostgreSQL database. It validates incoming rows against a JSON schema, applies configurable rejection logic, and logs all outcomes to structured CSV logs for analysis.

This tool prioritizes **control**, **auditability**, and **data integrity**, with a design aimed at technical users who need visibility into the ingestion process.

---

## Features

* **Schema-based column validation** with rules for:

  * Presence (required)
  * Data type enforcement and casting
  * Regex format compliance
  * Value whitelisting/blacklisting
  * Min/max limits
* **Group-level (order-level) rejection** and **cascade failure control**
* **Detailed, structured logs**:

  * Discrete log files for different log types (`INGEST`, `ERROR`, `EXCEPTION`, etc.)
  * Optional merged log file (`MERGED`)
* **Configurable verbosity** through `log_profile` in `config.json`
* **Automatic crash logs** capturing the full session buffer on failure
* **PostgreSQL integration** for writing validated data
* **CLI interface** via `run_ingestor.py`

---

## Current Limitations

* **Schema, config, and database validation** functions are stubbed (not yet implemented)
* **No GUI** (planned)
* **No schema auto-generation** (planned)
* **No multi-database support**—PostgreSQL only
* **No chunked processing (`elephant_mode`) yet**—planned for large datasets

---

## File Layout

```
run_ingestor.py            # CLI wrapper
src/
  main.py                  # Pipeline orchestration
  file_loader.py           # CSV file loading and schema alignment
  validator.py             # Validation loop and execution engine
  validation_library.py    # Validation rule functions and type mapping
  db_writer.py             # PostgreSQL insert logic
  logger.py                # Logging and crash handling
config/
  config.json              # Runtime and log configuration
  schema.json              # Validation schema
logs/                      # Generated log files
```

---

## Configuration

### `config.json` (simplified view)

```json
{
  "runtime_config": {
    "user_id": "user_id",
    "cascade_reject": true,
    "drop_extra_cols": true,
    "log_config": {
      "log_to_console": true,
      "merge_logs": true,
      "log_filename": "logs/{session_id}_{log_type}_LOG.csv",
      "log_profile": {
        "validation_reject": true,
        "validation_warn": true,
        "validation_accept": false,
        "error_critical": true,
        "error_minor": true,
        "info_general": true,
        "info_detailed": false,
        "function_call": false,
        "procedure_status": true
      }
    }
  },
  "db_config": { ... },
  "schema_path": "config/schema.json"
}
```

### `schema.json` (simplified view)

```json
{
  "primary_key": "order_id",
  "group_reject": false,
  "schema_definitions": {
    "order_id": { "rules": { "required": true, "data_type": "TEXT", ... } },
    "quantity": { "rules": { "data_type": "INTEGER", "limit": { "min": 1 } } },
    ...
  }
}
```

---

## Logging System

* Logs are **structured CSV files** (one per `log_type`, plus optional merged log).
* Each log entry includes:

  * `timestamp`
  * `session_id`
  * `user_id`
  * `log_type` (e.g., `INGEST`, `ERROR`)
  * `log_class` (e.g., `validation_reject`, `error_critical`)
  * `message`
  * `called_by` (function name)
* **Crash logs** are written automatically on exception, dumping the full session buffer for post-mortem analysis.
* **Console logging** is optional (`log_to_console` toggle).

---

## Usage

Run from the CLI:

```bash
python run_ingestor.py --csv path/to/file.csv --config path/to/config.json
```

Defaults:

* `csv_path`: `sample_data/stresstest1.csv`
* `config_path`: `config/config.json`

---

## Roadmap

* Finalize `schema_validator`, `validate_config`, and `validate_database` functions
* Refactor `log_profile` to a more user-friendly toggle structure (optional enhancement)
* Implement `elephant_mode` (chunked processing for large files)
* Add GUI and schema generation utilities
* Polish naming conventions and perform a final consistency pass

---

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
