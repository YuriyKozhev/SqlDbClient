from typing import Optional, List
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

    def get_views(self, schema: Optional[str] = None) -> List[str]:
        """ Wrapping around sqlalchemy Inspector get_view_names function, and since version 2.0.0
        get_materialized_view_names_function.

        :param schema: schema in database
        :return: list of view names
        """
        views = self.get_view_names()
        if hasattr(self, 'get_materialized_view_names'):
            try:
                views += self.get_materialized_view_names()
            except NotImplementedError:
                pass
        return views


def inspect(*args, **kwargs) -> DBInspector:
    """Wrapper around sqlalchemy inspect function, that adds custom methods."""
    inspector = sqlalchemy.inspect(*args, **kwargs)
    setattr(inspector, 'get_columns_repr', types.MethodType(DBInspector.get_columns_repr, inspector))
    setattr(inspector, 'print_columns', types.MethodType(DBInspector.print_columns, inspector))
    setattr(inspector, 'get_views', types.MethodType(DBInspector.get_views, inspector))
    return inspector
