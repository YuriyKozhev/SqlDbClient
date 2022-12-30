import logging

from sqldbclient.sql_executor import SqlExecutor
from sqldbclient.dialects.postgresql.sql_view_factory.view import View, ViewType

logger = logging.getLogger(__name__)


class SqlViewMaterializerUtils:
    def __init__(self, view: View, sql_executor: SqlExecutor):
        self.view = view
        self.sql_executor = sql_executor

    def set_owner(self) -> None:
        if self.view.view_type == ViewType.REGULAR_VIEW:
            self.sql_executor.execute(f"""
                ALTER VIEW {self.view.full_name} OWNER TO {self.view.owner};
            """)
        elif self.view.view_type == ViewType.MATERIALIZED_VIEW:
            self.sql_executor.execute(f"""
                ALTER MATERIALIZED VIEW {self.view.full_name} OWNER TO {self.view.owner};
            """)
        else:
            raise Exception('Unexpected error')
        logger.info(f'View {self.view.full_name} owner set to {self.view.owner}')

    def set_privileges(self) -> None:
        for grantee, privileges in self.view.privileges.items():
            for privilege in privileges:
                self.sql_executor.execute(f"""
                    GRANT {privilege} ON {self.view.full_name} TO {grantee};
                """)
        logger.info(f'View {self.view.full_name} privileges set')

    def restore(self) -> None:
        self.create()
        self.set_owner()
        self.set_privileges()

    def drop(self) -> None:
        if self.view.view_type == ViewType.REGULAR_VIEW:
            self.sql_executor.execute(f'DROP VIEW {self.view.full_name}')
        elif self.view.view_type == ViewType.MATERIALIZED_VIEW:
            self.sql_executor.execute(f'DROP MATERIALIZED VIEW {self.view.full_name}')
        else:
            raise Exception('Unexpected error')
        logger.info(f'View {self.view.full_name} dropped')

    def create(self) -> None:
        if self.view.view_type == ViewType.REGULAR_VIEW:
            query = '\n'.join([f'CREATE VIEW {self.view.full_name} AS', self.view.definition])
            self.sql_executor.execute(query)
        elif self.view.view_type == ViewType.MATERIALIZED_VIEW:
            query = '\n'.join([f'CREATE MATERIALIZED VIEW {self.view.full_name} AS',
                               self.view.definition.replace(';', ''),
                               'WITH NO DATA'])
            self.sql_executor.execute(query)
        else:
            raise Exception('Unexpected error')
        logger.info(f'Created {self.view.full_name}')

    def copy_privileges_to(self, obj: View):
        for grantee, privileges in self.view.privileges.items():
            for privilege in privileges:
                self.sql_executor.execute(f"""
                    GRANT {privilege} ON {obj.full_name} TO {grantee};
                """)

    def refresh(self) -> None:
        logger.info(f'Refreshing {self.view.full_name}...')
        if self.view.view_type == ViewType.REGULAR_VIEW:
            logger.info(f'Skipping regular view {self.view.full_name}')
        elif self.view.view_type == ViewType.MATERIALIZED_VIEW:
            self.sql_executor.execute(f'REFRESH MATERIALIZED VIEW {self.view.full_name}')
        else:
            raise Exception('Unexpected error')
        logger.info(f'Refreshed {self.view.full_name}')

    def drop_indexes(self) -> None:
        logger.info(f'Dropping indexes for {self.view.full_name}...')
        for index in self.view.indexes:
            self.sql_executor.execute(f'''
                DROP INDEX "{index['schema']}"."{index['name']}"
            ''')
        logger.info(f'Dropped indexes for {self.view.full_name}')

    def create_indexes(self) -> None:
        logger.info(f'Creating indexes for {self.view.full_name}...')
        for index in self.view.indexes:
            self.sql_executor.execute(index['definition'])
        logger.info(f'Created indexes for {self.view.full_name}')
