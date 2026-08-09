import json
from importlib.resources import files

from jsonschema import Draft202012Validator

_SCHEMA_PATH = files(__package__) / "_protocol.schema.json"
_schema = json.loads(_SCHEMA_PATH.read_text())
_validator = Draft202012Validator(_schema)

PROTOCOL_VERSION = 1


def validate_message(value):
    errs = list(_validator.iter_errors(value))
    if errs:
        return False, [f"{list(e.path)} {e.message}" for e in errs]
    return True, []
