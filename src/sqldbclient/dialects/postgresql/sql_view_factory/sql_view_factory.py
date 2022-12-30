import logging

import pandas as pd

from sqldbclient.dialects.postgresql.sql_view_factory.view import View, ViewType
from sqldbclient.dialects.postgresql.sql_view_factory.pg_info_queries import *
from sqldbclient.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)


def extract_dependant_objects(name: str, schema: str, sql_executor: SqlExecutor) -> pd.DataFrame:
    df = sql_executor.execute(PG_OBJECT_DEPENDENCIES_TEMPLATE.format(name=name, schema=schema))
    logger.info(f'Found {len(df)} dependant objects for "{schema}"."{name}"')
    # BFS-like algorithm
    for _, row in df.iterrows():
        df = pd.concat([df, extract_dependant_objects(row.dependent_view, row.dependent_schema, sql_executor)])
    return df


class SqlViewFactory:
    def __init__(self, view_name: str, view_schema: str, sql_executor: SqlExecutor):
        self.name = view_name
        self.schema = view_schema
        self.sql_executor = sql_executor
        self.parameters = dict(name=view_name, schema=view_schema)

    def _get_indexes(self) -> None:
        df = self.sql_executor.execute(PG_OBJECT_INDEXES_TEMPLATE.format(name=self.name, schema=self.schema))
        self.parameters['indexes'] = list(df.to_dict(orient='index').values())

    def _get_dependant_objects(self) -> None:
        dependencies = extract_dependant_objects(self.name, self.schema, self.sql_executor)
        dependant_objects = dependencies.apply(
            lambda row: SqlViewFactory(row['dependent_view'], row['dependent_schema'], self.sql_executor).create(),
            axis=1
        ).values.tolist()
        self.parameters['dependant_objects'] = dependant_objects

    def _get_privileges(self) -> None:
        df = self.sql_executor.execute(PG_OBJECT_PRIVILEGES_TEMPLATE.substitute(name=self.name, schema=self.schema))
        self.parameters.update(df.set_index('grantee').to_dict())

    @staticmethod
    def _extract_main_parameters(result: pd.DataFrame) -> dict:
        if len(result) > 1:
            raise Exception('Unexpected error while extracting main view parameters')
        renamed = result.rename(columns={
            'viewowner': 'owner',
            'matviewowner': 'owner',
        })
        prepared = renamed[['owner', 'definition']].iloc[0]
        parameters = prepared.to_dict()
        return parameters

    def _get_main_parameters(self) -> None:
        views_df = self.sql_executor.execute(PG_VIEWS_INFO_TEMPLATE.format(name=self.name, schema=self.schema))
        matviews_df = self.sql_executor.execute(PG_MATVIEWS_INFO_TEMPLATE.format(name=self.name, schema=self.schema))
        if not views_df.empty:
            main_parameters = self._extract_main_parameters(views_df)
            self.parameters.update(main_parameters)
            self.parameters['view_type'] = ViewType.REGULAR_VIEW
        elif not matviews_df.empty:
            main_parameters = self._extract_main_parameters(matviews_df)
            self.parameters.update(main_parameters)
            self.parameters['view_type'] = ViewType.MATERIALIZED_VIEW
        else:
            raise Exception(f'View object "{self.schema}"."{self.name}" not found')

    def create(self) -> View:
        self._get_main_parameters()
        self._get_privileges()
        self._get_dependant_objects()
        self._get_indexes()
        view_object = View(**self.parameters)
        return view_object
