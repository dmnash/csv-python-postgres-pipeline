# db_writer.py

import psycopg2
from psycopg2.extras import execute_values
from src.logger import log_event

def write_to_db(runtime_config, db_config, cleaned_data):
    dest_table = db_config['table']
    columns = list(cleaned_data.columns)
    insert_statement = f"INSERT INTO {dest_table} ({', '.join(columns)}) VALUES %s"
    insert_data = [tuple(row) for row in cleaned_data.to_numpy()]
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["name"],
            user=db_config["user"],
            password=db_config["password"]
        )
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_statement, insert_data)
                log_event(runtime_config, {
                    "message": f"data written to {dest_table}",
                    "log_type": "EVENT",
                    "log_class": "procedure_status",
                    "called_by": "db_writer.py"
                    })
    except Exception as e:
        log_event(runtime_config, {
            "message": f"failed to write to {dest_table}: {e}",
            "log_type": "ERROR",
            "log_class": "error_critical",
            "called_by": "db_writer.py"
            })
    finally:
        if "conn" in locals():
            conn.close()

'''
{"message": "","log_type": "","log_class": "","called_by": ""}
'''