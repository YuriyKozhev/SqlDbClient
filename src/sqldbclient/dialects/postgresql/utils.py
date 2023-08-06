from sqldbclient.sql_executor import SqlExecutor


def grant_access(
    object_name: str,
    object_schema: str,
    user_name: str,
    sql_executor: SqlExecutor,
    privilege: str = 'SELECT'
):
    with sql_executor:
        sql_executor.execute(f'''
            GRANT USAGE ON SCHEMA "{object_schema}" TO "{user_name}"
        ''')
        sql_executor.execute(f'''
            GRANT {privilege} ON "{object_schema}"."{object_name}" TO "{user_name}"
        ''')
        sql_executor.commit()
