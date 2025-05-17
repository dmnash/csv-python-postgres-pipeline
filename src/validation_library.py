# validation_library.py

import pandas as pd
import re
from src.logger import log_event

def schema_validator(schema: dict) -> tuple[bool, str]:
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

def build_dispatch_table(runtime_config):
    return {
        "required": function_wrangler(runtime_config)(valid_required),
        "format": function_wrangler(runtime_config)(format_compliance),
        "permitted_value": function_wrangler(runtime_config)(permitted_value),
        "data_type": function_wrangler(runtime_config)(valid_datatype),
        "limit": function_wrangler(runtime_config)(limit_value)
    }

def function_wrangler(runtime_config):
    def decorator(validation_func):
        def wrapper(schema_rule, test_value, col_name=None, source_index=None):
            try:
                result = validation_func(schema_rule, test_value)
                result.setdefault("col_name", col_name)
                result.setdefault("source_index", source_index)
                result["message"] = f"row {result['source_index']} rejected at {result['col_name']}: {result['message']}"
                if result.get("log"):
                    log_event(runtime_config, result.get("message"), result.get("log_level", "INGEST"))
                return result
            except Exception as e:
                log_event(runtime_config, f"{validation_func.__name__} failed: {e}", "EXCEPTION")
                return {"valid": False, "log": True, "message": f"{validation_func.__name__} failed: {e}", "log_level": "EXCEPTION", "col_name": col_name, "source_index": source_index}
        return wrapper
    return decorator

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
    "DATE": str,      # note: you'd likely parse this if you want to enforce date logic
    "TIMESTAMP": str, # same here
    "TIME": str
}

def valid_required(schema_rule, test_value):
# this will be the exists-and-not-null function that required fields all apply
    if not schema_rule:
        return {"valid": True, "log": False, "message": "no test", "log_level": "INGEST"}
    if test_value is None or pd.isnull(test_value):
        return {"valid": False, "log": True, "message": f"{test_value} is missing or null", "log_level": "INGEST"}
    return {"valid": True, "log": False, "message": f"{test_value} exists and is not null", "log_level": "INGEST"}

def format_compliance(schema_rule, test_value):
# this will be the regex format compliance test
    if not re.match(schema_rule, str(test_value)):
        return {"valid": False, "log":True, "message": f"{test_value} does not comply with format {schema_rule}", "log_level": "INGEST"}
    return {"valid": True, "log": False, "message": f"{test_value} complies with format {schema_rule}", "log_level": "INGEST"}

def permitted_value(schema_rule, test_value):
# this will be the accepted values and forbidden values tests
    if schema_rule.get("accepted_value") is not None and test_value in schema_rule["accepted_value"]:
        return {"valid": True, "log": False, "message": f"{test_value} is a permitted value", "log_level": "INGEST"}
    if schema_rule.get("forbidden_value") is not None and test_value in schema_rule["forbidden_value"]:
        return {"valid": False, "log": True, "message": f"{test_value} is a forbidden value", "log_level": "INGEST"}
    if schema_rule.get("accepted_value") is not None:
        return {"valid": False, "log": True, "message": f"{test_value} is not an accepted value", "log_level": "INGEST"}
    return {"valid": True, "log": False, "message": f"{test_value} is a permitted value", "log_level": "INGEST"}

def valid_datatype(schema_rule, test_value):
# this will be the data type validation test
    expected_type = schema_rule.upper()
    if expected_type not in POSTGRES_PYTHON_DATA_MAP:
        return {"valid": False, "log": True, "message": f"Unsupported data type: {expected_type}", "log_level": "INGEST"}
    if isinstance(test_value, POSTGRES_PYTHON_DATA_MAP[expected_type]):
        return {"valid": True, "log": False, "message": f"{test_value} is valid {expected_type}", "log_level": "INGEST"}
    try:
        cast_value = POSTGRES_PYTHON_DATA_MAP[expected_type](test_value)
        return {"valid": True, "log": True, "message":  f"{test_value} cast from {type(test_value).__name__.lower()} to {expected_type}", "log_level": "INGEST"}
    except Exception as e:
        return {"valid": False, "log": True, "message": f"{test_value} is not castable to {expected_type} ({type(test_value).__name__.lower()})", "log_level": "INGEST"}

def limit_value(schema_rule, test_value):
# this will be the min/max value test
    if schema_rule.get("min") is not None:
        try:
            casted_value = type(schema_rule["min"])(test_value)
            if casted_value < schema_rule["min"]:
                return {"valid": False, "log": True, "message": f"{test_value} below minimum tolerance {schema_rule['min']}", "log_level": "INGEST"}
        except Exception as e:
            return {"valid": False, "log": True, "message": f"{test_value} cannot be evaluated against MIN rule", "log_level": "INGEST"}
    if schema_rule.get("max") is not None:
        try:
            casted_value = type(schema_rule["max"])(test_value)
            if casted_value > schema_rule["max"]:
                return {"valid": False, "log": True, "message": f"{test_value} exceeds maximum tolerance {schema_rule['max']}", "log_level": "INGEST"}
        except Exception as e:
            return {"valid": False, "log": True, "message": f"{test_value} cannot be evaluated against MAX rule", "log_level": "INGEST"}
    return {"valid": True, "log": False, "message": f"{test_value} is within accepted tolerance", "log_level": "INGEST"}