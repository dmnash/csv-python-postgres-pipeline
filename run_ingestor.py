# run_ingestor.py

import argparse
from main import main

def parse_args():
    parser = argparse.ArgumentParser(description="Run the CSV Ingestion Engine.")
    parser.add_argument("--csv", type=str, help="Path to CSV file", default="sample_data/stresstest1.csv")
    parser.add_argument("--config", type=str, help="Path to config.json", default="config/config.json")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    main(csv_path=args.csv, config_path=args.config)