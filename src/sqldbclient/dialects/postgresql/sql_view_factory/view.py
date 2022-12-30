import enum
from typing import Dict, List
from dataclasses import dataclass, field


class ViewType(enum.Enum):
    REGULAR_VIEW = 'v'
    MATERIALIZED_VIEW = 'vm'


@dataclass
class View:
    schema: str
    name: str
    full_name: str = field(init=False)
    view_type: ViewType
    owner: str
    definition: str = field(repr=False)
    privileges: Dict[str, List[str]] = field(repr=False)
    dependant_objects: List['View'] = field(repr=False)
    dependant_objects_number: int = field(init=False)
    indexes: List[Dict[str, str]] = field(repr=False)
    indexes_number: int = field(init=False)

    def __post_init__(self):
        self.full_name = f'"{self.schema}"."{self.name}"'
        self.dependant_objects_number = len(self.dependant_objects)
        self.indexes_number = len(self.indexes)
