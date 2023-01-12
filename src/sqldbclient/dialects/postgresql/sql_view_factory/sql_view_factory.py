import logging
import queue

import pandas as pd

from sqldbclient.dialects.postgresql.sql_view_factory.view import View, ViewType
from sqldbclient.dialects.postgresql.sql_view_factory.pg_info_queries import *
from sqldbclient.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)


def pre_traverse(name: str, schema: str, sql_executor: SqlExecutor) -> pd.DataFrame:
    df = sql_executor.execute(PG_OBJECT_DEPENDENCIES_TEMPLATE.format(name=name, schema=schema))
    logger.info(f'Found {len(df)} dependant objects for "{schema}"."{name}"')
    # dependant objects tree traversal in pre-order (that is, object first, its dependencies second)
    for _, row in df.iterrows():
        df = pd.concat([df, pre_traverse(row.dependent_view, row.dependent_schema, sql_executor)])
    return df


def breadth_first_traverse(name: str, schema: str, sql_executor: SqlExecutor) -> pd.DataFrame:
    pass


def extract_dependant_objects(name: str, schema: str, sql_executor: SqlExecutor) -> pd.DataFrame:
    dependencies = pre_traverse(name, schema, sql_executor).reset_index(drop=True)
    dependencies['explored'] = False
    root = pd.Series({
        'dependent_schema': schema,
        'dependent_view': name,
        'source_schema': schema,
        'source_table': name,
        'explored': True
    })
    q = queue.Queue()
    q.put(root)
    values = []
    while not q.empty():
        obj = q.get(block=False)
        values.append(obj)
        deps = dependencies[
            (dependencies['source_schema'] == obj['dependent_schema']) &
            (dependencies['source_table'] == obj['dependent_view'])
        ]
        for idx, dep in deps.iterrows():
            if not dependencies.loc[idx, 'explored']:
                dependencies.loc[
                    (dependencies['dependent_schema'] == dep['source_schema']) &
                    (dependencies['dependent_view'] == dep['source_table']),
                    'explored'
                ] = True
                q.put(dep)
    values.pop(0)
    if values:
        result = pd.concat(values, axis=1).T
    else:
        result = pd.DataFrame({}, columns=['dependent_view', 'dependent_schema'])
    return result


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
