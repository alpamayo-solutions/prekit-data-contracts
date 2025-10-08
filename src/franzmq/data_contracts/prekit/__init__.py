from .payload import (
    DataTag, DataTags, SignalData, 
    DataTagContext, DataTagContexts, ServiceDetails, 
    Computation, ComputationContext, ComputationContexts, 
    ArgMapping, ValidationResults)

__all__ = ["DataTag", "DataTags", "SignalData", "DataTagContext", "DataTagContexts",
           "ServiceDetails", "Computation", "ComputationContext", "ComputationContexts", 
           "ArgMapping", "ValidationResults"]

# Monkey Patching JSONEncoder
_original_encoder = json.JSONEncoder.default
def _patched_encoder(self, o):
    if isinstance(o, enum.Enum):
        return str(o)
    return _original_encoder(self, o)
json.JSONEncoder.default = _patched_encoder