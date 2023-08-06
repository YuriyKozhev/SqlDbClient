import json
from sqlalchemy import TypeDecorator, String


class DataTypes(TypeDecorator):
    impl = String

    def process_literal_param(self, value, dialect):
        """Convert dict of mapping between pandas DataFrame columns and their data types to json-string.

        :param value: dict of mapping between pandas DataFrame columns and their data types
        :param dialect: sqlalchemy dialect
        :return: json-string
        """
        return json.dumps(value, default=str)

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Converts json-string to dict of mapping between pandas DataFrame columns and their data types.

        :param value: json-string
        :param dialect:sqlalchemy dialect
        :return: dict of mapping between pandas DataFrame columns and their data types
        """
        return json.loads(value)
