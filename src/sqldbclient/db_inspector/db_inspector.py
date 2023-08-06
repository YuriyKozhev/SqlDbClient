from typing import Optional
import types

import sqlalchemy
from sqlalchemy.engine.reflection import Inspector


class DBInspector(Inspector):
    def get_columns_repr(self, table: str, schema: Optional[str] = None) -> str:
        """Constructs string with structured information about table columns and their data types.

        :param table: table name
        :param schema: schema name
        """
        output = f'"{table}"\n'
        if schema:
            output = f'"{schema}".' + output
        prefix = ' |-- '
        for c in self.get_columns(table, schema):
            output += f"{prefix}{c['name']}: {c['type']} "
            output += f"({', '.join(f'{k}={v}' for k, v in c.items() if k not in ('name', 'type'))})\n"
        return output

    def print_columns(self, table: str, schema: Optional[str] = None) -> None:
        """Prints structured information about table columns and their data types.

        :param table: table name
        :param schema: schema name
        """
        print(self.get_columns_repr(table, schema))


def inspect(*args, **kwargs) -> DBInspector:
    """Wrapper around sqlalchemy inspect function, that adds custom methods."""
    inspector = sqlalchemy.inspect(*args, **kwargs)
    setattr(inspector, 'get_columns_repr', types.MethodType(DBInspector.get_columns_repr, inspector))
    setattr(inspector, 'print_columns', types.MethodType(DBInspector.print_columns, inspector))
    return inspector
