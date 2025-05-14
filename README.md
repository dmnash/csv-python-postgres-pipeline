# CSV to PostgreSQL Ingestion Engine

# Ingestion Engine

The Ingestion Engine is a modular, schema-driven pipeline for validating and ingesting structured CSV data into a PostgreSQL database. It performs field-level validation based on a JSON schema, logs processing outcomes, and writes clean data into a configured table.

This tool is designed for technical users who want control, visibility, and auditability in their data ingestion process—without manually writing one-off scripts.

---

## Features

* Schema-based column validation
* Trinary rule handling (rejected / warn / accepted)
* Structured logging with support for grouped or row-level rejection
* Centralized config for runtime behavior
* CLI interface via `run_ingestor.py`
* PostgreSQL target (no other DBs supported)

---

## Known Limitations

* The schema validator is not yet functional (schema files are assumed to be well-formed)
* Logging is verbose and unstructured; overhaul is in progress
* Only supports PostgreSQL as a destination
* No GUI yet (planned)
* No schema auto-generation or assistive tooling

---

## Installation

1. Clone the repository
2. Create a virtual environment:

   ```
   python -m venv venv
   ```
3. Activate the environment:

   ```
   # Windows
   venv\Scripts\activate
   # macOS/Linux
   source venv/bin/activate
   ```
4. Install requirements:

   ```
   pip install -r requirements.txt
   ```

---

## Usage

Run the ingestion engine from the command line:

```
python run_ingestor.py --csv sample_data/stresstest1.csv --config config/config.json
```

If no arguments are provided, defaults will be used:

* CSV path: `sample_data/stresstest1.csv`
* Config path: `config/config.json`

---

## File Layout

```
run_ingestor.py       # CLI wrapper  
main.py               # Ingestion pipeline entry point  
config/  
  config.json         # Runtime behavior configuration  
  schema.json         # Schema-based column rules  
logger.py             # Logging utility  
validator.py          # Data validation logic  
db_writer.py          # Postgres insert logic  
internal_docs/  
  stitchlog.txt       # Development log  
```

---

## Config Overview

The ingestion behavior is controlled by `config/config.json`. Key fields include:

* `input_path`: default CSV file
* `db_config`: database connection and table name
* `cascade_reject`: halt row/group on first failure
* `group_reject`: treat grouped rows atomically

---

## Schema Overview

The schema file (`schema.json`) defines:

* Which columns are required
* What data type they must conform to
* Optional regex and range validation rules
* Whether permitted/forbidden value lists are enforced

---

## Logging

Logs are written to the path defined in `config.json`.
Current logging includes all rejections, warnings, and status updates.
Logging output is verbose by default and subject to future refinement.

---

## License

This project is licensed under the [Apache 2.0 License](LICENSE).