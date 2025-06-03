# run_ingestor.py

import argparse
import sys
from src.main import main, initialize_config

def parse_args():
    parser = argparse.ArgumentParser(description="Run the CSV Ingestion Engine.")
    parser.add_argument("--csv", type=str, help="Path to CSV file", default="")
    parser.add_argument("--config", type=str, help="Path to config.json", default="config/config.json")
    return parser.parse_args()

if __name__ == "__main__":
    try:
        args = parse_args()
        configs = initialize_config(config_path=args.config)
        main(csv_path=args.csv, runtime_context=configs)
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)