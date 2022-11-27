import pandas as pd

try:
    from IPython.display import display
except ImportError as e:
    raise ImportError('Pandas DataFrame full_display method available only for Jupyter environment')


full_display_config = {
    'display.max_rows': 1_000,
    'display.max_columns': 1_000,
    'display.max_colwidth': 500,
}


class TooBigToDisplayException(Exception):
    pass


def full_display(self, cols=True, rows=True, width=False):
    max_rows = full_display_config['display.max_rows']
    max_columns = full_display_config['display.max_columns']
    max_colwidth = full_display_config['display.max_colwidth']

    if rows and self.shape[0] > max_rows:
        raise TooBigToDisplayException(f'DataFrame has too many rows: {self.shape[0]} (> {max_rows})')
    if cols and self.shape[1] > max_columns:
        raise TooBigToDisplayException(f'DataFrame has too many columns: {self.shape[1]} (> {max_columns})')

    cols_num = max_columns if cols else pd.get_option("display.max_columns")
    rows_num = max_rows if rows else pd.get_option("display.max_rows")
    width_num = max_colwidth if width else pd.get_option("display.max_colwidth")

    with pd.option_context('display.max_rows', rows_num):
        with pd.option_context('display.max_columns', cols_num):
            with pd.option_context('display.max_colwidth', width_num):
                display(self)


setattr(pd.DataFrame, 'full_display', full_display)
