import io

from sqlalchemy import TypeDecorator, String
import pandas as pd

from sqldbclient.utils.pandas.parse_dates import parse_dates


class DataFrame(TypeDecorator):
    impl = String

    def process_literal_param(self, value, dialect):
        """Converts value pandas DataFrame to csv-like string.

        :param value: pandas DataFrame
        :param dialect: sqlalchemy dialect
        :return: csv-like string
        """
        return value.to_csv(sep='\x1F', index=False)

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Converts csv-like string to pandas DataFrame and casts columns to pandas datetime when applicable.

        :param value: csv-like string
        :param dialect: sqlalchemy dialect
        :return: pandas DataFrame
        """
        buffer = io.StringIO(value)
        result = pd.read_csv(buffer, sep='\x1F')  # noqa
        result = parse_dates(result)
        return result
