# logger.py

from datetime import datetime
import os

log_library = {
    "INGEST" : os.path.join("logs", "ingestion_log.txt"),
    "ERROR" : os.path.join("logs", "error_log.txt"),
    "EVENT" : os.path.join("logs", "event_log.txt")
}

EXCEPTION_LOG = os.path.join("logs", "exception_log.txt")

def log_event(runtime_config, message, log_type):
    """
    Append a timestamped message to the log.

    Args:
        message (str): The log message to record.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {message}\n"

    # Identify event type
    if log_type in log_library:
        # Write to designated log file
        with open(log_library[log_type], "a") as log:
            log.write(entry)
    else:
        # Write to overflow log file
        with open(EXCEPTION_LOG, "a") as log:
            log.write(f"[{timestamp}] {message}; logged as {log_type.upper()}\n")

    # Print to console for dev
    print(entry.strip())

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3:
        message = sys.argv[1]
        log_type = sys.argv[2]
        log_event(message, log_type)
    else:
        print("Usage: python src/logger.py <message> <log_type>")
