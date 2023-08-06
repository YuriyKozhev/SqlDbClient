from typing import Optional

import pandas as pd

try:
    from sqlalchemy.engine.cursor import CursorResult
except ImportError:
    # support for legacy sqlalchemy versions (< 1.4)
    from sqlalchemy.engine.result import ResultProxy as CursorResult

from sqldbclient.utils.pandas.parse_dates import parse_dates


def cursor_result_to_df(cursor_result: CursorResult, force_result_fetching: bool = False) -> Optional[pd.DataFrame]:
    if not cursor_result.returns_rows and not force_result_fetching:
        return None
    df = pd.DataFrame(cursor_result.fetchall(), columns=list(cursor_result.keys()))
    df = parse_dates(df)
    return df
