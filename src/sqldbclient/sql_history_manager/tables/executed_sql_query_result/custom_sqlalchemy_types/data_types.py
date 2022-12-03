import json
from sqlalchemy import TypeDecorator, String


class DataTypes(TypeDecorator):
    impl = String

    def process_literal_param(self, value, dialect):
        return json.dumps(value, default=str)

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        return json.loads(value)
