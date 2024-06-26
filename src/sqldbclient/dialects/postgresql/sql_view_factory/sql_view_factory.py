import logging
from typing import List, Dict, Tuple, Optional

import pandas as pd

from sqldbclient.dialects.postgresql.sql_view_factory.view import View, ViewType
from sqldbclient.dialects.postgresql.sql_view_factory.pg_info_queries import PG_VIEWS_INFO_TEMPLATE, \
    PG_OBJECT_DEPENDENCIES_TEMPLATE, PG_OBJECT_INDEXES_TEMPLATE, PG_MATVIEWS_INFO_TEMPLATE, \
    PG_OBJECT_PRIVILEGES_TEMPLATE, PG_OBJECT_DESCRIPTIONS_TEMPLATE
from sqldbclient.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)


def pre_traverse(name: str, schema: str, sql_executor: SqlExecutor) -> pd.DataFrame:
    """Collects object dependencies, dependencies of its dependencies, and etc.

    :param name: object name
    :param schema: object schema
    :param sql_executor: instance of SqlExecutor
    :return:
    """
    df = sql_executor.execute(PG_OBJECT_DEPENDENCIES_TEMPLATE.format(name=name, schema=schema))
    logger.info(f'Found {len(df)} dependant objects for "{schema}"."{name}"')
    # dependant objects tree traversal in pre-order (that is, object first, its dependencies second)
    for _, row in df.iterrows():
        df = pd.concat([df, pre_traverse(row.dependent_view, row.dependent_schema, sql_executor)])
    return df


def depth_first_traverse(dependencies: pd.DataFrame, obj: pd.Series, values: List[pd.Series]) -> None:
    """Sorts dependencies from children to parents.

    :param dependencies: pandas DataFrame
    :param obj: pandas Series
    :param values: list of sorted objects
    """
    dependencies.loc[
        (dependencies['dependent_schema'] == obj['dependent_schema']) &
        (dependencies['dependent_view'] == obj['dependent_view']),
        'explored'
    ] = True
    deps = dependencies[
        (dependencies['source_schema'] == obj['dependent_schema']) &
        (dependencies['source_table'] == obj['dependent_view'])
        ]
    for idx, dep in deps.iterrows():
        if not dependencies.loc[idx, 'explored']:
            depth_first_traverse(dependencies, dep, values)
    values.append(obj)


def extract_dependant_objects(name: str, schema: str, sql_executor: SqlExecutor) -> pd.DataFrame:
    """Extracts dependencies ordered from parents to children.

    :param name: object name
    :param schema: object schema
    :param sql_executor: instance of SqlExecutor
    :return:
    """
    dependencies = pre_traverse(name, schema, sql_executor).reset_index(drop=True)
    dependencies['explored'] = False
    root = pd.Series({
        'dependent_schema': schema,
        'dependent_view': name,
        'source_schema': schema,
        'source_table': name,
        'explored': True
    })
    values = []
    depth_first_traverse(dependencies, root, values)
    values = values[:-1]  # remove root
    values = values[::-1]  # reverse order
    if values:
        result = pd.concat(values, axis=1).T
    else:
        result = pd.DataFrame({}, columns=['dependent_view', 'dependent_schema'])
    return result


class SqlViewFactory:
    """Factory to create View objects, which store all information obout them
    to be able to fully restore them in database, if necessary.
    """
    def __init__(self, view_name: str, view_schema: str, sql_executor: SqlExecutor):
        self.name = view_name
        self.schema = view_schema
        self.sql_executor = sql_executor
        self.parameters = dict(name=view_name, schema=view_schema)
        self._cached_views: Optional[Dict[Tuple[str, str], View]] = None

    def _get_indexes(self) -> None:
        df = self.sql_executor.execute(PG_OBJECT_INDEXES_TEMPLATE.format(name=self.name, schema=self.schema))
        self.parameters['indexes'] = list(df.to_dict(orient='index').values())

    def _get_dependant_objects(self) -> None:
        dependencies = extract_dependant_objects(self.name, self.schema, self.sql_executor)
        dependant_objects = []
        for _, row in dependencies.iterrows():
            obj_factory = SqlViewFactory(row['dependent_view'], row['dependent_schema'], self.sql_executor)
            obj_factory._cached_views = self._cached_views
            obj = obj_factory.create()
            dependant_objects.append(obj)
        self.parameters['dependant_objects'] = dependant_objects

    def _get_privileges(self) -> None:
        df = self.sql_executor.execute(PG_OBJECT_PRIVILEGES_TEMPLATE.substitute(name=self.name, schema=self.schema))
        self.parameters.update(df.set_index('grantee').to_dict())

    def _get_descriptions(self) -> None:
        df = self.sql_executor.execute(PG_OBJECT_DESCRIPTIONS_TEMPLATE.format(name=self.name, schema=self.schema))
        self.parameters['table_description'] = df.iloc[0]['table_description']
        self.parameters['col_descriptions'] = df.iloc[0]['col_descriptions']

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
        """Creates View object with all necessary information.

        :return: View object
        """
        # None when called from user code, Dict when called within this class
        if self._cached_views is None:
            self._cached_views = {}

        if (self.schema, self.name) in self._cached_views:
            logger.info(f'Used cached version of View with schema = "{self.schema}" and name = "{self.name}"')
            return self._cached_views[(self.schema, self.name)]

        self._get_main_parameters()
        self._get_privileges()
        self._get_dependant_objects()
        self._get_indexes()
        self._get_descriptions()
        view_object = View(**self.parameters)

        # cache will be used by non-direct and secondary parents of the View
        self._cached_views[(self.schema, self.name)] = view_object
        # when View is created there is no need to store reference to cache
        # moreover, cache should be invalidated when user use the same SqlViewFactory another time
        self._cached_views = None

        return view_object
