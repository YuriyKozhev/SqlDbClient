import io

from sqlalchemy import TypeDecorator, String
import pandas as pd

from sqldbclient.utils.pandas.parse_dates import parse_dates


class DataFrame(TypeDecorator):
    impl = String

    def process_literal_param(self, value, dialect):
        return value.to_csv(sep='\x1F', index=False)

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        buffer = io.StringIO(value)
        result = pd.read_csv(buffer, sep='\x1F')  # noqa
        result = parse_dates(result)
        return result
