# CSV to PostgreSQL Ingestion Engine

## Ingestion Engine

The Ingestion Engine is a modular, schema-driven pipeline for validating and ingesting structured CSV data into a PostgreSQL database. It performs field-level validation based on a JSON schema, logs processing outcomes, and writes clean data into a configured table.

This tool is designed for technical users who want control, visibility, and auditability in their data ingestion process—without manually writing one-off scripts.

---

## Features

* Schema-based column validation
* Each rule can either pass or fail, with optional logging of successes, warnings, or failures
* Structured logging for all validation events
* Group-level or row-level rejection logic (schema-driven)
* Config-driven runtime behavior
* CLI interface via `run_ingestor.py`
* PostgreSQL target (no other DBs supported)

---

## Known Limitations

* Schema validation tooling (`validate_schema`) is stubbed but not implemented
* Config and DB connection validation are also stubbed
* Logging is currently always enabled and verbose
* Only supports PostgreSQL as a destination
* GUI is not yet implemented (planned)
* No schema auto-generation or assistive tooling

---

## Installation

1. Clone the repository
2. Create a virtual environment:

   ```bash
   python -m venv venv
   ```
3. Activate the environment:

   ```bash
   # Windows
   venv\Scripts\activate
   # macOS/Linux
   source venv/bin/activate
   ```
4. Install requirements:

   ```bash
   pip install -r requirements.txt
   ```

---

## Usage

Run the ingestion engine from the command line:

```bash
python run_ingestor.py --csv sample_data/stresstest1.csv --config config/config.json
```

If no arguments are provided, defaults will be used:

* CSV path: `sample_data/stresstest1.csv`
* Config path: `config/config.json`

---

## File Layout

```bash
run_ingestor.py            # CLI entry point
main.py                    # Pipeline orchestration
src/
  file_loader.py           # CSV file loading and schema alignment
  validator.py             # Validation loop and execution engine
  validation_library.py    # Rule functions and database type mapping
  db_writer.py             # PostgreSQL insert logic
  logger.py                # Logging utility
config/
  config.json              # Runtime configuration
  schema.json              # Validation schema
```

---

## Config Overview

The ingestion behavior is controlled by `config/config.json`. Key fields include:

* `csv_path`: populated at runtime
* `schema_path`: location of the JSON schema file
* `db_config`: connection and destination table
* `runtime_config`:

  * `cascade_reject`: stop evaluating a row/group on first failure
  * `group_reject`: treat grouped rows as a single atomic unit

---

## Schema Overview

The schema file (`schema.json`) defines:

* Column presence and required status
* Data types and PostgreSQL compatibility
* Optional regex and range rules
* Permitted and forbidden value lists

---

## Logging

Logs are generated automatically during each run. They record:

* All rule violations and warnings
* Row- and group-level outcomes
* Summary-level ingestion metrics (planned)

Logging output is always active. Verbosity controls and file routing are planned.

---

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
