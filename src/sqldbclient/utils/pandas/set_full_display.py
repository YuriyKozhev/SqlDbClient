from typing import Optional

import pandas as pd

try:
    from IPython.display import display
except ImportError:
    raise ImportError('Pandas DataFrame full_display method available only for Jupyter environment')


class TooBigToDisplayException(Exception):
    """Exception to raise if the pandas.DataFrame, that is being displayed, is too big"""
    pass


def set_full_display(
    max_rows: Optional[int] = 1000,
    max_columns: Optional[int] = 1000,
    max_colwidth: Optional[int] = 500,
    display_all_columns_by_default: Optional[bool] = True,
    display_all_rows_by_default: Optional[bool] = True,
    display_whole_colwidth_by_default: Optional[bool] = False,
) -> None:
    """ Sets method ``full_display`` to a pandas.DataFrame object with configured parameters

    :param max_rows: Maximum number of rows to display fully.
        If a pandas.DataFrame has more rows than max_rows, a ``TooBigToDisplayException`` is raised.
    :param max_columns: Maximum number of columns to display fully.
        If a pandas.DataFrame has more columns than max_columns, a ``TooBigToDisplayException`` is raised.
    :param max_colwidth: Maximum column width to display.
        If a pandas.DataFrame column is wider, the content will be suppressed to max_colwidth characters.
    :param display_all_columns_by_default: If ``True``,
        `full_display` method will try to display all columns by default.
    :param display_all_rows_by_default: If ``True``,
        `full_display` method will try to display all rows by default.
    :param display_whole_colwidth_by_default: If ``True``,
        ``full_display`` method will try to display whole column width by default.
    """
    def full_display(self,
                     cols=display_all_columns_by_default,
                     rows=display_all_rows_by_default,
                     width=display_whole_colwidth_by_default):
        """Displays pandas DataFrame with full numbers of rows and columns.
            If the numbers are too high (> 1000, by default), an exception is raised.

            :param self:
            :param cols: If ``True``, tries to display all columns.
            :param rows: If ``True``, tries to display all rows.
            :param width:If ``True``, tries to display full content of each cell.
            """

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
