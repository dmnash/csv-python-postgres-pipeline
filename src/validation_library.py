# validation_library.py

import pandas as pd
import re

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

def validate_schema():
    '''
    Validate that MIN and MAX rules don't conflict.
    Validate that DATA_TYPE requirement references valid data type
    Validate that REGEX requirement syntax is valid
    Validate that PERMITTED_VALUE rules don't invalidate each other, warn on partial overlap
    Validate that RULE is known
    '''
    schema_valid = True
    validation_msg = ""
    return schema_valid, validation_msg

def validate_config():
    '''
    Validate config settings
    '''
    schema_valid = True
    validation_msg = ""
    return schema_valid, validation_msg

def validate_database():
    '''
    Validate database settings
    '''
    schema_valid = True
    validation_msg = ""
    return schema_valid, validation_msg

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