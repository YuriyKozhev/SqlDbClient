from typing import Optional
import types

import sqlalchemy
from sqlalchemy.engine.reflection import Inspector


class DBInspector(Inspector):
    def print_columns(self, table: str, schema: Optional[str] = None) -> None:
        output = f'"{table}"\n'
        if schema:
            output = f'"{schema}".' + output
        prefix = ' |-- '
        for c in self.get_columns(table, schema):
            output += f"{prefix}{c['name']}: {c['type']} "
            output += f"({', '.join(f'{k}={v}' for k, v in c.items() if k not in ('name', 'type'))})\n"
        print(output)


def inspect(*args, **kwargs) -> DBInspector:
    inspector = sqlalchemy.inspect(*args, **kwargs)
    setattr(inspector, 'print_columns', types.MethodType(DBInspector.print_columns, inspector))
    return inspector
