# logger.py

from datetime import datetime
import os
import csv

log_library = ["INGEST", "ERROR", "EVENT", "EXCEPTION"]

'''
EXCEPTION_LOG = os.path.join("logs", "exception_log.txt")
'''

def log_event(runtime_config, log_entry):
    checked_entry = log_entry.copy()
    checked_entry.update({"timestamp": datetime.now().isoformat(), "user_id": runtime_config["user_id"], "session_id": runtime_config["session_id"]})

    # Lines 19 - 27 are developer-facing check to catch malformed function calls
    required = ["log_type", "log_class", "message"]    
    default_entries = {"log_type": "EXCEPTION", "log_class": "error_critical", "message": "undefined", "called_by": "undefined"}

    malformed = any(key not in checked_entry for key in required)
    for key in default_entries:
        checked_entry.setdefault(key, default_entries[key])

    if malformed or checked_entry["log_type"] not in log_library:
        checked_entry["log_type"] = "EXCEPTION"
    # end malformed function call validation check

    runtime_config["log_buffer"].append(checked_entry)

    if runtime_config["log_config"]["log_to_console"]:
        print(checked_entry)

def write_to_logs(runtime_config):
    log_config = runtime_config["log_config"]
    logs_by_type = {"INGEST": {"fieldnames": set(), "data_rows": list()},
                    "ERROR": {"fieldnames": set(), "data_rows": list()},
                    "EXCEPTION": {"fieldnames": set(), "data_rows": list()}
                    }
    log_buffer = runtime_config["log_buffer"]

    if log_config["merge_logs"]:
        logs_by_type["MERGED"] = {"fieldnames": set(), "data_rows": list()}

    for log_entry in log_buffer:
        if log_entry["log_class"] in log_config["log_profile"]:
            logs_by_type[log_entry["log_type"]]["fieldnames"].update(log_entry.keys())
            logs_by_type[log_entry["log_type"]]["data_rows"].append(log_entry)
            if log_config["merge_logs"]:
                logs_by_type["MERGED"]["fieldnames"].update(log_entry.keys())
                logs_by_type["MERGED"]["data_rows"].update(log_entry.keys())

    for compiled_log, log_data in logs_by_type:
        log_file = log_config["log_filename"].format(session_id=runtime_config["session_id"], log_type=compiled_log)
        headers = list(log_data["fieldnames"])
        with open(log_file, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            for row in log_data["data_rows"]:
                writer.writerow(row)
                if log_config["log_to_console"]: print(row)
        print(f"Log File Written: {log_file} ({len(log_data['data_rows'])} entries)")
    
    runtime_config["log_buffer"] = []

    return

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3:
        message = sys.argv[1]
        runtime_config = {
            "log_config": {
                "log_to_console": True,
                "log_to_file": False
            },
            "file_info": {
                "path": "manual"
            }
        }
        log_event(runtime_config, message)
    else:
        print("Usage: python src/logger.py <message> <log_type>")
