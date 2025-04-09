# main.py

from src.file_loader import load_csv
from src.validator import validate_data
from src.db_writer import write_to_db
from src.logger import log_event

def main():
    # Step 1: Load raw data from a CSV file
    raw_data = load_csv("sample_data/example1.csv")

    # Step 2: Validate and clean the data
    cleaned_data = validate_data(raw_data)

    # Step 3: Write validated data to the database
    write_to_db(cleaned_data)

    # Step 4: Log completion
    log_event("Ingestion complete.")

if __name__ == "__main__":
    main()
