import sqlite3
import os

import pytest

from sqldbclient.sql_executor import SqlExecutorConf, SqlExecutor

TEST_SQLITE_DB_NAME = 'test_sqlite_tmp.db'
TEST_HISTORY_DB_NAME = 'test_history_tmp.db'


@pytest.fixture
def sql_executor():
    sqlite3.connect(TEST_SQLITE_DB_NAME).close()
    yield SqlExecutor.builder.config(
        SqlExecutorConf()
        .set('engine_options', f'sqlite:///{TEST_SQLITE_DB_NAME}')
        .set('history_db_name', TEST_HISTORY_DB_NAME)
    ).get_or_create()
    os.remove(TEST_SQLITE_DB_NAME)
    os.remove(TEST_HISTORY_DB_NAME)


def test_execute_and_transaction_management(sql_executor):
    sql_executor.execute('CREATE TABLE t (c INTEGER)')
    with sql_executor:
        sql_executor.execute('INSERT INTO t VALUES (1)')
    with sql_executor:
        sql_executor.execute('INSERT INTO t VALUES (2)')
        sql_executor.commit()
    cnt_df = sql_executor.execute('SELECT count(*) AS cnt FROM t')
    assert cnt_df.cnt.iloc[0] == 1
