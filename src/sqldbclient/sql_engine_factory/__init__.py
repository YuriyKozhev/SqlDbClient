"""
One can use ``sql_engine_factory`` to create sqlalchemy engines and avoid resource leakage
by keeping only one engine per a unique set of parameters:

  .. code-block:: python

   from sqldbclient import sql_engine_factory

   # pass arguments and keyword arguments as to sqlalchemy create_engine function

   sqlite_engine = sql_engine_factory.get_or_create('sqlite:///my_sqlite.db')

"""

from sqldbclient.sql_engine_factory.sql_engine_factory import SqlEngineFactory

sql_engine_factory = SqlEngineFactory()
