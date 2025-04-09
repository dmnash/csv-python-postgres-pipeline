# CSV to PostgreSQL Ingestion Engine

This project is a modular, testable pipeline for ingesting CSV files into a PostgreSQL database. It is designed for flexibility, maintainability, and clarity, with logging support for ingestion, event flow, and error conditions.

The pipeline is written in Python and intended as a portfolio project demonstrating data engineering fundamentals.

---

## 🚀 Features

- Modular architecture using separate files for loading, validation, logging, and DB writing
- Clean CLI-driven entry point (`main.py`) with default behavior and override support
- Separate logging for ingestion flow, events, and error states
- Logging fail-safes to ensure all events are captured, even on unexpected input
- Project-structured for extensibility and unit test support

---

## 📁 File Structure

```
ingestion_engine/
├── logs/                     # All logs written here
│   ├── ingestion_log.txt
│   ├── event_log.txt
│   ├── error_log.txt
│   └── exception_log.txt     # Catches malformed logging calls
│
├── sample_data/              # Input CSV files for testing/demo
│   └── example1.csv
│
├── src/                      # Modular code structure
│   ├── file_loader.py
│   ├── validator.py
│   ├── db_writer.py
│   └── logger.py
│
├── main.py                   # Pipeline entry point
├── requirements.txt
├── LICENSE
└── README.md
```

---

## 📦 Requirements

This project requires Python 3.10+ and the following packages (see `requirements.txt`):

- `pandas`
- `psycopg2` (for PostgreSQL integration — stubbed for now)

To install:
```bash
pip install -r requirements.txt
```

---

## 🛠️ Usage

From the project root:
```bash
python main.py sample_data/example1.csv
```

If no argument is provided, it will default to `sample_data/example1.csv`.

---

## 📋 Logging System

Logging is handled via `src/logger.py`. Events are written to one of the following logs:

| Log File            | Purpose                                                        |
|--------------------|----------------------------------------------------------------|
| `ingestion_log.txt`| Tracks ingestion actions (e.g., file loaded, rows processed)   |
| `event_log.txt`    | Tracks high-level pipeline events (e.g., validation started)   |
| `error_log.txt`    | Tracks all error messages and recoverable failures             |
| `exception_log.txt`| Captures unexpected logging calls or failures (invalid type)   |

All logs are timestamped. Log files are written in plain text and appended to incrementally.

---

## 🔮 Planned Improvements

- Implement validation rules in `validator.py`
- Wire up real DB logic in `db_writer.py`
- Add unit tests and CI support
- Add file-watching daemon or scheduling logic
- Implement schema versioning and config files

---

## 📄 License

This project is licensed under the [Apache 2.0 License](LICENSE).