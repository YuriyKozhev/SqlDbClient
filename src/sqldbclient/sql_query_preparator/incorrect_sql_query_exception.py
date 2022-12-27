class IncorrectSqlQueryException(Exception):
    def __init__(self, msg: str):
        msg = f'SQL query is incorrect: {msg}'
        super(IncorrectSqlQueryException, self).__init__(msg)
