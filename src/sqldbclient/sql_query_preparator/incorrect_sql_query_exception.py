class IncorrectSqlQueryException(Exception):
    """Exception to raise if parsed query is incorrect"""
    def __init__(self, msg: str):
        msg = f'SQL query is incorrect: {msg}'
        super(IncorrectSqlQueryException, self).__init__(msg)
