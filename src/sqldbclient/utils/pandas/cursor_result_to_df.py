from typing import Optional

import pandas as pd

try:
    from sqlalchemy.engine.cursor import CursorResult
except ImportError:
    # support for legacy sqlalchemy versions (< 1.4)
    from sqlalchemy.engine.result import ResultProxy as CursorResult

from sqldbclient.utils.pandas.parse_dates import parse_dates


def cursor_result_to_df(cursor_result: CursorResult, force_result_fetching: bool = False) -> Optional[pd.DataFrame]:
    """ Fetches rows from cursor_result when it returns them,
    and creates pandas DataFrame.

    :param cursor_result: CursorResult that is obtained from calling sqlalchemy execute method
    :param force_result_fetching: If ``True``, will try to fetch rows from cursor result that is obtained
            after executing query, even when the type of query does not imply returning any rows.
    :return: (optional) If query selects any rows then a pandas DataFrame will be returned.
    """
    if not cursor_result.returns_rows and not force_result_fetching:
        return None
    df = pd.DataFrame(cursor_result.fetchall(), columns=list(cursor_result.keys()))
    df = parse_dates(df)
    return df
