from sqldbclient.sql_executor import SqlExecutor


def grant_access(
    object_name: str,
    object_schema: str,
    user_name: str,
    sql_executor: SqlExecutor,
    privilege: str = 'SELECT'
) -> None:
    """Grants privilege on object to user

    :param object_name: object name in database
    :param object_schema: object schema in database
    :param user_name: user_name in database
    :param sql_executor: instance of SqlExecutor
    :param privilege: one of { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | REFERENCES | TRIGGER }
    """
    with sql_executor:
        sql_executor.execute(f'''
            GRANT USAGE ON SCHEMA "{object_schema}" TO "{user_name}"
        ''')
        sql_executor.execute(f'''
            GRANT {privilege} ON "{object_schema}"."{object_name}" TO "{user_name}"
        ''')
        sql_executor.commit()
