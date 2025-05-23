# main.py

import sys
import json
import src.validation_library as vl
from src.file_loader import load_csv
from src.validator import validate_data
from src.db_writer import write_to_db
from src.logger import log_event

def main(csv_path="sample_data/stresstest1.csv", config_path="config/config.json"):
    # load config
    with open(config_path, "r") as f:
        config = json.load(f)

    runtime_config = config["runtime_config"]
    runtime_config["csv_path"] = csv_path
    db_config = config["db_config"]
    schema_path = config["schema_path"]

    # TODO: vl.validate_config() will called here
    
    # TODO: vl.validate_database() will called here

    # load schema
    with open(schema_path, "r") as f:
        schema = json.load(f)

    # TODO: vl.validate_schema() will be called here
    
    schema_keys = set(schema["schema_definitions"].keys())

    # Load raw data from a CSV file
    raw_data = load_csv(runtime_config, schema_keys)
    if raw_data is None:
        log_event(runtime_config, "Aborting pipeline: no data loaded.","ERROR")
        return

    # Validate and clean the data
    cleaned_data = validate_data(runtime_config, schema, raw_data)

    # Write validated data to the database
    log_event(runtime_config, f"Sending {len(cleaned_data)} rows to database", "INGEST")
    write_to_db(runtime_config, db_config, cleaned_data)

    # Log completion
    log_event(runtime_config, "Ingestion complete.","INGEST")

if __name__ == "__main__":
    # Accept a filepath as a command-line argument, fallback to default
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()