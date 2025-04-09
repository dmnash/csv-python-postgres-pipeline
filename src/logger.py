# logger.py

from datetime import datetime
import os

LOG_FILE = os.path.join("logs", "ingestion_log.txt")

def log_event(message):
    """
    Append a timestamped message to the ingestion log.

    Args:
        message (str): The log message to record.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {message}\n"

    # Write to log file
    with open(LOG_FILE, "a") as log:
        log.write(entry)

    # Optional: print to console for dev
    print(entry.strip())
