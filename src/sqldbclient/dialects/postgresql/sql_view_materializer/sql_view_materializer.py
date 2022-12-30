from dataclasses import fields

from sqldbclient.sql_executor import SqlExecutor
from sqldbclient.dialects.postgresql.sql_view_factory.view import View
from sqldbclient.dialects.postgresql.sql_view_factory.sql_view_factory import SqlViewFactory
from sqldbclient.dialects.postgresql.sql_view_materializer.sql_view_materializer_utils import SqlViewMaterializerUtils
from sqldbclient.utils.log_decorators import logger


class SqlViewMaterializer:
    def __init__(self, view: View, sql_executor: SqlExecutor):
        self.sql_executor = sql_executor
        self.view = view
        self.existing_view = SqlViewFactory(self.view.name, self.view.schema, self.sql_executor).create()

    def _recreate(self):
        with self.sql_executor:
            # from children to parents
            dependant_objects_reversed = self.existing_view.dependant_objects[::-1]
            for obj in dependant_objects_reversed:
                SqlViewMaterializerUtils(obj, self.sql_executor).drop()
            SqlViewMaterializerUtils(self.existing_view, self.sql_executor).drop()

            # from parents to children
            SqlViewMaterializerUtils(self.view, self.sql_executor).restore()
            for obj in self.view.dependant_objects:
                SqlViewMaterializerUtils(obj, self.sql_executor).restore()

            # from parents to children
            SqlViewMaterializerUtils(self.view, self.sql_executor).refresh()
            for obj in self.view.dependant_objects:
                SqlViewMaterializerUtils(obj, self.sql_executor).refresh()

            self.sql_executor.commit()
        logger.warning(f'View {self} recreated')

    def _parse_field(self, field):
        existing_value = getattr(self.existing_view, field.name)
        new_value = getattr(self.view, field.name)
        if existing_value == new_value:
            return
        logger.warning(f'Found different value for field: {field.name}')

        if field.name in ['schema', 'name', 'full_name']:
            raise Exception('Unexpected error')
        if field.name in ['dependant_objects', 'dependant_objects_number']:
            logger.warning(f'{field.name} cannot be changed')
            return

        if field.name == 'privileges':
            SqlViewMaterializerUtils(self.view, self.sql_executor).set_privileges()
        elif field.name == 'owner':
            SqlViewMaterializerUtils(self.view, self.sql_executor).set_owner()
        elif field.name in ['definition', 'view_type']:
            logger.warning(f'To change {field.name} view {self.view.full_name} will be fully recreated')
            self._recreate()

        logger.warning(f'Field {field.name} set to {new_value}')

    def materialize(self) -> None:
        if self.view == self.existing_view:
            logger.warning("View already exists. Exiting...")
            return

        if fields(self.view) != fields(self.existing_view):
            raise ValueError(f'View {self.view} invalid')

        for field in fields(self.view):
            self._parse_field(field)
