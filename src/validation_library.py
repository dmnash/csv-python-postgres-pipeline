# validation_library.py

import pandas as pd
import psycopg2
import re
from psycopg2.extras import execute_values

def build_dispatch_table():
    return {
        "required": valid_required,
        "format": format_compliance,
        "permitted_value": permitted_value,
        "data_type": valid_datatype,
        "limit": limit_value
    }

# Validation functions to test schema requirements are defined in this section.
# TODO: Refactor all validation functions to return {"valid": bool, "log": bool}

def valid_required(schema_rule, test_value):
# exists-and-not-null test
    if test_value is None or pd.isnull(test_value):
        return {
            "valid": False, "log_class": "validation_reject", "called_by": "valid_required",
            "message": f"is missing or null"
            }
    return {
        "valid": True, "log_class": "validation_accept", "called_by": "valid_required",
        "message": f"exists and is not null"
        }

def format_compliance(schema_rule, test_value):
# regex format compliance test
    if not re.match(schema_rule, str(test_value)):
        return {
            "valid": False, "log_class": "validation_reject", "called_by": "format_compliance",
            "message": f"does not comply with format {schema_rule}"
            }
    return {
        "valid": True, "log_class": "validation_accept", "called_by": "format_compliance",
        "message": f"complies with format {schema_rule}"
        }

def permitted_value(schema_rule, test_value):
# accepted values and forbidden values tests
    if schema_rule.get("accepted_value") is not None and test_value in schema_rule["accepted_value"]:
        return {
            "valid": True, "log_class": "validation_accept", "called_by": "permitted_value",
            "message": f"{test_value} is an accepted value"
            }
    if schema_rule.get("forbidden_value") is not None and test_value in schema_rule["forbidden_value"]:
        return {
            "valid": False, "log_class": "validation_reject", "called_by": "permitted_value",
            "message": f"{test_value} is a forbidden value"
            }
    if schema_rule.get("accepted_value") is not None:
        return {
            "valid": False, "log_class": "validation_reject", "called_by": "permitted_value",
            "message": f"{test_value} is not an accepted value"
            }
    return {
        "valid": True, "log_class": "validation_accept",  "called_by": "permitted_value",
        "message": f"{test_value} is an accepted value"
        }

def valid_datatype(schema_rule, test_value):
# data type validation test
    expected_type = schema_rule.upper()
    if expected_type not in POSTGRES_PYTHON_DATA_MAP:
        return {
            "valid": False, "log_class": "validation_reject", "called_by": "valid_datatype",
            "message": f"Unsupported data type: {expected_type}"
            }
    if isinstance(test_value, POSTGRES_PYTHON_DATA_MAP[expected_type]):
        return {
            "valid": True, "log_class": "validation_accept",  "called_by": "valid_datatype",
            "message": f"valid {expected_type}"
            }
    try:
        cast_value = POSTGRES_PYTHON_DATA_MAP[expected_type](test_value)
        return {
            "valid": True, "log_class": "validation_warn",  "called_by": "valid_datatype",
            "message":  f"cast from {type(test_value).__name__.lower()} to {expected_type}"
            }
    except Exception as e:
        return {
            "valid": False, "log_class": "validation_reject", "called_by": "valid_datatype",
            "message": f"not castable to {expected_type} ({type(test_value).__name__.lower()})"
            }

def limit_value(schema_rule, test_value):
# min/max value test
    if schema_rule.get("min") is not None:
        try:
            casted_value = type(schema_rule["min"])(test_value)
            if casted_value < schema_rule["min"]:
                return {
                    "valid": False, "log_class": "validation_reject", "called_by": "limit_value",
                    "message": f"below MIN tolerance {schema_rule['min']}"
                    }
        except Exception as e:
            return {
                "valid": False, "log_type": "ERROR", "log_class": "validation_reject", "called_by": "limit_value",
                "message": f"cannot evaluate MIN rule ({str(e)})"
                }
    if schema_rule.get("max") is not None:
        try:
            casted_value = type(schema_rule["max"])(test_value)
            if casted_value > schema_rule["max"]:
                return {
                    "valid": False, "log_class": "validation_reject", "called_by": "limit_value",
                    "message": f"exceeds MAX tolerance {schema_rule['max']}"
                    }
        except Exception as e:
            return {
                "valid": False, "log_type": "ERROR", "log_class": "validation_reject", "called_by": "limit_value",
                "message": f"cannot evaluate MAX rule ({str(e)})"
                }
    return {
        "valid": True, "log_class": "validation_accept", "called_by": "limit_value",
        "message": f"within MIN/MAX"
        }

# Functions to validate configuration, schema, and database are defined here.

def validate_schema(schema):
    dispatch_table = build_dispatch_table()
    validation_report = ""
    for fieldname, fielddef in schema["schema_definitions"].items():
        data_type = fielddef.get("data_type")
        if data_type is not None and data_type not in POSTGRES_PYTHON_DATA_MAP:
            validation_report += f"{fieldname}: unrecognized data type '{data_type}'\n"
        for rule_name, rule_params in fielddef.get("rules", {}).items():
            if rule_name not in dispatch_table:
                validation_report += f"{fieldname}: unknown validation rule {rule_name}"
            if rule_name == "limit":
                if rule_params.get("min") is not None and rule_params.get("max") is not None:
                    if rule_params["min"] >= rule_params["max"]:
                        validation_report += f"{fieldname}: 'min' >= 'max' in limit\n"
            if rule_name == "permitted_values":
                accepted = rule_params.get("accepted_values", [])
                forbidden = rule_params.get("forbidden_values", [])
                overlap = set(accepted) & set(forbidden)
                if overlap:
                    validation_report += f"{fieldname}: 'accepted_values' and 'forbidden_values' overlap\n"
    if len(validation_report) != 0:
        raise RuntimeError(f"invalid schema:\n{validation_report}")

def validate_config():
    '''
    Validate config settings
    '''
    config_valid = True
    validation_msg = ""
    return config_valid, validation_msg

def validate_database(db_config):
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["name"],
            user=db_config["user"],
            password=db_config["password"]
        )
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {db_config['table']} LIMIT 1;")
        conn.close()
    except Exception as e:
        raise RuntimeError(f"database validation failed: {e}")

# Data type maps for different database formats are defined in this section.

POSTGRES_PYTHON_DATA_MAP = {
    "TEXT": str,
    "VARCHAR": str,
    "CHAR": str,
    "INTEGER": int,
    "INT": int,
    "SMALLINT": int,
    "BIGINT": int,
    "REAL": float,
    "FLOAT": float,
    "DOUBLE PRECISION": float,
    "NUMERIC": float,
    "BOOLEAN": bool,
    "DATE": str,      # NB: more complex data type validation needs to be built
    "TIMESTAMP": str,
    "TIME": str
}