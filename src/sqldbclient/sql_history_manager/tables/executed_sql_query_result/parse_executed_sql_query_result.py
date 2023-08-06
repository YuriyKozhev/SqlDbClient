import pandas as pd
from sqldbclient.sql_history_manager.tables.executed_sql_query_result.executed_sql_query_result \
    import ExecutedSqlQueryResult


def parse_executed_sql_query_result(result: ExecutedSqlQueryResult) -> pd.DataFrame:
    """Restores original column data types of dumped pandas DataFrame

    :param result: ExecutedSqlQueryResult
    :return: pandas DataFrame
    """
    df = result.dataframe
    for i, col in enumerate(df.columns):
        df[col] = df[col].astype(result.datatypes[i])
    return df
