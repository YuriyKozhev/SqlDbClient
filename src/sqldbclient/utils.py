import pandas as pd


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    original_cols = df.columns
    df.columns = [f'col_{i}' for i in range(len(df.columns))]
    for col in df.columns:
        if df[col].dtypes != object or df[col].isnull().all() or (df[col] == '').all():
            continue
        try:
            df[col] = pd.to_datetime(df[col])
        except:
            pass
    df.columns = original_cols
    return df
