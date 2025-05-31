# main.py

import sys
import json
import uuid
import csv
import src.validation_library as vl
from datetime import datetime
from src.file_loader import load_csv
from src.validator import validate_data
from src.db_writer import write_to_db
from src.logger import log_event, write_to_logs

def main(csv_path="", runtime_context=""):

    runtime_config = runtime_context["runtime_config"]
    schema_path = runtime_context["schema_path"]
    db_config = runtime_context["db_config"]

    # runtime setup
    runtime_config["session_id"] = str(uuid.uuid4())
    runtime_config["log_buffer"] = []
    runtime_config["csv_path"] = csv_path
    log_profile = list()
    for key, keyval in runtime_config["log_config"]["log_profile"].items():
        if keyval == True:
            log_profile.append(key)
    runtime_config["log_config"]["log_profile"] = log_profile

    # schema setup
    schema = initialize_schema(schema_path)
    schema_keys = set(schema["schema_definitions"].keys())

    try:
        # Load raw data from a CSV file
        raw_data = load_csv(runtime_config, schema_keys)
        if raw_data is None:
            log_event(runtime_config, {
                "message": "Aborting pipeline: no data loaded.",
                "log_type": "ERROR",
                "log_class": "error_critical",
                "called_by": "main.py"})
            return

        # Validate ~~and clean~~ the data
        cleaned_data = validate_data(runtime_config, schema, raw_data)

        # Write validated data to the database
        log_event(runtime_config, {
            "message": f"Sending {len(cleaned_data)} rows to database",
            "log_type": "EVENT",
            "log_class": "function_call",
            "called_by": "main.py"
            })
        write_to_db(runtime_config, db_config, cleaned_data)

        # Log completion
        log_event(runtime_config, {
            "message": f"{csv_path} process complete",
            "log_type": "EVENT",
            "log_class": "procedure_status",
            "called_by": "main.py"
            })
        
        write_to_logs(runtime_config)

    except Exception as e:
        crashrow = {"timestamp": datetime.now().isoformat(), "user_id": runtime_config["user_id"], "message": f"critical error: {e}", }
        crashlog_name = f"logs/CRASH_{runtime_config['session_id']}.csv"
        log_buffer = runtime_config["log_buffer"]
        fieldnames = ["timestamp","message"]
        for log_entry in log_buffer:
            fieldnames += [key for key in log_entry.keys() if key not in fieldnames]
        with open(crashlog_name, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for log_entry in log_buffer:
                writer.writerow(log_entry)
            writer.writerow(crashrow)
        print(f"critical error: {e}\ncrash log written to {crashlog_name}")

def initialize_config(config_path="config/config.json"):
    runtime_context = {}
    with open(config_path, "r") as f:
        config = json.load(f)

    runtime_context["runtime_config"] = config["runtime_config"]
    runtime_context["db_config"] = config["db_config"]
    runtime_context["schema_path"] = config["schema_path"]

    # TODO: vl.validate_config() will be called here
    
    vl.validate_database(runtime_context["db_config"])

    return runtime_context    

def initialize_schema(schema_path):
    schema = {}
    with open(schema_path, "r") as f:
        schema = json.load(f)

    vl.validate_schema(schema)

    return schema

if __name__ == "__main__":
    # Accept a filepath as a command-line argument, fallback to default
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()