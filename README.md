# CSV to PostgreSQL Ingestion Engine

## Overview

This **Ingestion Engine** is a schema-driven Python pipeline that validates structured CSV data and ingests it into a PostgreSQL database. It checks rows against a user-configurable JSON schema, applies configurable rejection rules, and writes structured logs to CSV files for later analysis. 

This is my first software project, an exercise in designing, building, and debugging a non-trivial Python tool from scratch. It’s unpolished, but it functions. It’s intended for technical users who need a transparent, configurable ingestion tool that validates data before it goes into a database.

---

## Features

* **Schema-based column validation** with validation rule support for:

  * Presence (required)
  * Data type enforcement and casting
  * Regex format compliance
  * Value whitelisting/blacklisting
  * Min/max limits
* **Group-level (order-level) rejection** and **cascade failure control**
* **Detailed, structured logs**:

  * Discrete log files for different log types (`INGEST`, `ERROR`, `EVENT`, `EXCEPTION`, etc.)
  * Optional merged log file (`MERGED`)
* **Configurable log suppression** by `log_class` through `log_profile` in `config.json`
* **Automatic crash logs** capturing the full session buffer on failure
* **PostgreSQL integration** for writing validated data
* **CLI interface** via `run_ingestor.py`
* **Database validation** on startup
* **Schema validation** on startup

---

## Current Limitations

* **No multi-database support**—PostgreSQL only
* **No chunked processing** to handle large datasets

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

### `config.json`

Settings in `config.json` file control runtime behavior, logging, and database connection details for the Ingestion Engine. Users should review and customize these values to match their environment and desired behavior.

#### Top-level Structure

```json
{
  "runtime_config": { ... },
  "db_config": { ... },
  "schema_path": "config/schema.json"
}
```

Each section is described below in more detail.

---

### `runtime_config`

Controls general ingestion behavior and logging settings.

```json
{
  "user_id": "user_id",     	// User identifier stamped into logs for traceability
  "session_id": "",	        	// Populated automatically at runtime (leave empty)
  "csv_path": "",	          	// Populated automatically at runtime (leave empty)
  "cascade_reject": true,   	// Stop validation after first failure in a row
  "drop_extra_cols": true,    // Drop any columns not defined in the schema
  "log_config": { ... }       // See log_config section below
}
```

---

### `log_config`

Controls logging behavior, output files, and filtering of log events by type.

```json
{
  "log_to_console": true,          // Print logs to the console during execution
  "merge_logs": true,              // Combine all logs into a single merged log file
  "log_filename": "logs/{session_id}_{log_type}_LOG.csv", // Template for log filenames
  "log_profile": {                 // Controls which log classes are written
    "validation_reject": true,     // Log rejected rows
    "validation_warn": true,       // Log information (e.g., data type casts)
    "validation_accept": false,    // Log accepted rows (discouraged)
    "error_critical": true,        // Log critical errors
    "error_minor": true,           // Log minor errors
    "info_general": true,          // Log general info events
    "info_detailed": false,        // Log detailed info events
    "function_call": false,        // Log every function call
    "procedure_status": true       // Log status updates for validation procedures
  }
}
```

WARNING: Setting all `log_profile` options to `true` will produce very large log files and may degrade performance.

---

### `db_config`

Database connection details for the PostgreSQL instance where validated data will be written.

```json
{
  "host": "localhost",           // PostgreSQL host address
  "port": 5432,                  // PostgreSQL port
  "name": "your_database",       // Database name
  "user": "your_username",       // Database user
  "password": "your_password",   // Database password
  "table": "your_table"          // Target table for validated data
}
```

---

### `schema_path`

Specifies the location of the JSON schema file used for validation.

```json
"schema_path": "config/schema.json"
```


### `schema.json` (simplified view)

```json
{
  "sort_key": "order_id",               // Rows are grouped by sort_key
  "group_reject": false,                // Reject group if one row fails validation
  "schema_definitions": {               // Dictionary of column names and their rules
    "column_name_1": {                  // Unique name for each column to be validated
      "rules": {                        // Library of rules applied to this column
        "required": true,               // Value exists and is not null
        "format": "^\\d{3}-\\d{4}$",    // Value complies with described regex format
        "value_restrictions": {         // "FORBID" or "ALLOW" certain values
          "FORBID": ["UNKNOWN", "N/A"]  // Value is compared against list entries
        },                              // 
        "data_type": "TEXT",            // Value is acceptable data type
        "limit": {                      //
          "min": 1,                     // Set allowable minimum or maximum values
          "max": 10                     // Can check only min or only max, or both
        }                               
        }
      },
    "column_name_2": {
      "rules": {
        "data_type": "INTEGER",
        "limit": { "min": 1 },
        ...
      }
    },
    ...
  }
}
```

---

## Logging System

* Logs are **structured CSV files** (one per `log_type`, plus optional merged log).
* Log file names follow the **log_filename** template in **config.json**
* Log entries contain these fields:
  * `timestamp`
  * `session_id`
  * `user_id`
  * `log_type` (e.g., `INGEST`, `ERROR`, `EVENT`, `EXCEPTION`)
  * `log_class` (e.g., `validation_reject`, `error_critical`; See log_config for classes)
  * `message`
  * `called_by` (function name)
* **Crash logs** are written automatically on exception, dumping the full session buffer for post-mortem analysis. **Crash logs** are highly verbose.
* **Console logging** is optional (`log_to_console` toggle).

---

## Usage

Run from the CLI:

```bash
python run_ingestor.py --csv path/to/file.csv --config path/to/config.json
```

If no path is provided for the **--config** argument the Ingestion Engine will default to using **config/config.json**

The **--csv** argument is not optional.

---

## Future Enhancements

* Implement chunked processing for large files (`elephant_mode`)
* Add GUI and schema generation utilities

---

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
