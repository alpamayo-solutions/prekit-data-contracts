import json 
import enum

# Monkey Patching JSONEncoder
_original_encoder = json.JSONEncoder.default
def _patched_encoder(self, o):
    if isinstance(o, enum.Enum):
        return str(o)
    return _original_encoder(self, o)
json.JSONEncoder.default = _patched_encoder