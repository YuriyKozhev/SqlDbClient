from sqlalchemy.engine.cursor import CursorResult
import pandas as pd

from sqldbclient.utils.pandas.parse_dates import parse_dates


def cursor_result_to_df(cursor_result: CursorResult) -> pd.DataFrame:
    df = pd.DataFrame(cursor_result.fetchall(), columns=list(cursor_result.keys()))
    df = parse_dates(df)
    return df
