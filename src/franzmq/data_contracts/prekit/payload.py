import json
import hashlib
import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from franzmq.data_contracts.base.payload import Payload, DataType, IndexType, ServiceType

@dataclass
class DataTag(Payload):
    id: str
    name: str
    is_writable: bool
    is_readable: bool
    data_type: Optional[str] = None
    hierarchy: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataTags(Payload):
    data_tags: List[DataTag]
    connector: str

    @property
    def version(self) -> str:
        sorted_tags = sorted(self.data_tags, key=lambda x: x.id)
        tags_dict = [tag.__dict__ for tag in sorted_tags]
        return hashlib.md5(json.dumps(tags_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTags':
        data = json.loads(json_str)
        data_tags = [DataTag(**tag) for tag in data["data_tags"]]
        data["data_tags"] = data_tags
        data.pop("version", None)
        return cls(**data)

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["data_tags"] = [tag.__dict__ for tag in self.data_tags]
        d["version"] = self.version
        return d


@dataclass
class SignalData:
    id: str
    name: str
    source: str
    data_type: DataType
    index_type: IndexType
    topic_name: str
    system_element: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    unit: Optional[str] = None
    precision: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    @property
    def hierarchy(self) -> List[str]:
        if self.system_element:
            return self.system_element.split(" - ")
        return []

    def encode(self):
        return json.dumps(self.__dict__)

@dataclass
class DataTagContext(Payload):
    id: str
    tag_id: str
    source: str
    topic_name: str
    is_logged: bool
    is_published: bool
    signal: SignalData

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTagContext':
        data = json.loads(json_str)
        data["signal"] = SignalData(**data["signal"])
        data.pop("version", None)
        return cls(**data)

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["signal"] = self.signal.__dict__
        return d


@dataclass
class DataTagContexts(Payload):
    data_tag_contexts: List[DataTagContext]

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTagContexts':
        data = json.loads(json_str)
        data_tag_contexts = [DataTagContext.decode(json.dumps(context), timestamp) for context in data["data_tag_contexts"]]
        return cls(data_tag_contexts=data_tag_contexts)

    @property
    def version(self) -> str:
        sorted_contexts = sorted(self.data_tag_contexts, key=lambda x: x.tag_id)
        contexts_dict = [context.__dict__ for context in sorted_contexts]
        return hashlib.md5(json.dumps(contexts_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["data_tag_contexts"] = [context.__dict__ for context in self.data_tag_contexts]
        d["version"] = self.version
        return d

@dataclass
class ServiceDetails(Payload):
    id: str
    service_type: ServiceType
    display_name: str | None = None
    version: str | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    hierarchy: List[str] = field(default_factory=list)



@dataclass
class Computation(Payload):
    id: str
    func_name: str
    func_hash: str
    description: str
    args: List[str]
    return_data_type: DataType
    return_index_type: IndexType
    source: str

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["return_data_type"] = str(self.return_data_type)
        d["return_index_type"] = str(self.return_index_type)
        return d


@dataclass
class Computations(Payload):
    computations: List[Computation]
    service: str

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'Computations':
        data = json.loads(json_str)
        computations = [Computation(**computation) for computation in data["computations"]]
        return cls(computations=computations, service=data["service"])

    @property
    def version(self) -> str:
        sorted_computations = sorted(self.computations, key=lambda x: x.func_name)
        computations_dict = [computation.__dict__ for computation in sorted_computations]
        return hashlib.md5(json.dumps(computations_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["computations"] = [computation.__dict__ for computation in self.computations]
        d["version"] = self.version
        return d

@dataclass
class ArgMapping:
    arg: str
    signal: SignalData | None
    is_trigger: bool


@dataclass
class ComputationContext(Payload):
    computation_id: str
    func_hash: str
    source: str
    func_name: str
    topic_name: str
    is_logged: bool
    is_published: bool
    on_interval_ms: int
    signal: SignalData
    arg_mappings: list[ArgMapping] # maps arg name to signal

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["signal"] = self.signal.__dict__
        d["arg_mappings"] = []
        for a in self.arg_mappings:
            a_dict = a.__dict__.copy()
            if a.signal is None:
                a_dict["signal"] = None
            else:
                a_dict["signal"] = a.signal.__dict__
            d["arg_mappings"].append(a_dict)
        return d


    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'ComputationContext':
        data = json.loads(json_str)
        data["signal"] = SignalData(**data["signal"])
        data["arg_mappings"] = [
            ArgMapping(
                arg=arg["arg"],
                signal=SignalData(**arg["signal"]) if arg["signal"] else None,
                is_trigger=arg["is_trigger"]
            ) for arg in data["arg_mappings"]
        ]
        return cls(**data)

@dataclass
class ComputationContexts(Payload):
    computation_contexts: List[ComputationContext]

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'ComputationContexts':
        data = json.loads(json_str)
        # Use the updated decode method of ComputationContext
        computation_contexts = [ComputationContext.decode(json.dumps(context), timestamp) for context in data["computation_contexts"]]
        return cls(computation_contexts=computation_contexts)

    @property
    def version(self) -> str:
        sorted_contexts = sorted(self.computation_contexts, key=lambda x: x.computation_id)
        contexts_dict = [context.__dict__ for context in sorted_contexts]
        return hashlib.md5(json.dumps(contexts_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["computation_contexts"] = [context.__dict__ for context in self.computation_contexts]
        d["version"] = self.version
        return d
        

@dataclass
class ValidationResults(Payload):
    service_id: str
    file_results: Dict[str, Dict[str, Any]]
    timestamp: float = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).timestamp())
    general_error: Optional[str] = None