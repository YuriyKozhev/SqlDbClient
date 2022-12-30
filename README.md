# Sql DB Client

Provides additional functionalities to work with DBMS utilizing powerful Python packages such as sqlalchemy and pandas.

The main goal to provide a handy alternative to basic SQL client software applications 
(e.g. [DBeaver](https://en.wikipedia.org/wiki/DBeaver), [pgAdmin](https://www.pgadmin.org/), etc.).

This package mostly aims at SQL scripts executing since other types of database related activities
(such as database navigation, objects structure and fields management) can done more conveniently with graphical UI.

Designed mainly to use inside Jupyter Notebook 
(i.e. some kind of GUI-like environment but with advantages of using Python and its libraries).
Especially helpful for people who are used to work with pandas 
since SQL queries results will be shown and saved in pandas.DataFrame format.

Currently, there are 4 main tools one most likely to use in their scripts:
- sql_executor module
- sql_asyncio module
- dialects.postgresql module
- db_inspector module
- handy utils

## Sql Executor

Either can be build from a config

      from sqldbclient import SqlExecutor, SqlExecutorConf

      sql_executor = SqlExecutor.builder.config(
          SqlExecutorConf()
              # pass arguments to sqlalchemy.create_engine function
              .set('engine_options', 'postgresql+psycopg2://postgres:mysecretpassword@localhost:5555', echo=False)
              # choose the name of sqlite database file with saved queries results
              .set('history_db_name', 'sql_executor_history.db')
              # set the maximum number of rows a SELECT query can fetch
              .set('max_rows_read', 10_000)
      ).get_or_create()

Or created explicitly, but sqlalchemy Engine needs to created first (also you can use SqlEngineFactory for that)
      
      import sqlalchemy
      from sqldbclient import SqlExecutor

      sqlite_engine = sqlalchemy.create_engine('sqlite:///my_sqlite.db')

      sql_executor = SqlExecutor(
        engine=sqlite_engine, 
        max_rows_read=10_000, 
        history_db_name='sql_executor_history.db'
      )

Though it is recommended to build it from a config since 
then SqlExecutor instance and corresponding sqlalchemy engine will be automatically cached.
It will ensure no leakage of resources if one try to create multiple instances.


Roughly speaking, it is a wrapper over pd.DataFrame().read_sql method but with the following features:
- Automatic SELECT queries preprocessing and limiting to a configured number (to help avoiding memory overuse) - via SqlQueryPreparator
      
      '''SELECT * from  
          some_table'

      '''select *    FROM some_table
       LIMIT too_large_limit'''

  The queries above will be transformed to the query below
      if the limit is not specified or exceeds the configured number

      '''SELECT * 
         FROM some_table
         LIMIT {max_rows_read}'''

- Easy transaction management (using context manager) - via SqlTransactionManager
        
        with sql_executor:
            sql_executor.execute('INSERT INTO some_table VALUES (1, 2, 3)')
            sql_executor.commit() #  otherwise the transaction will be automatically rolled back

- Query results storing in a SQLite database 
(i.e. a file inside a directory with your scripts)  - via SqlHistoryManager

  - _No need to save select results into csv and excel files in order to work with them in the future.
        They will be available in the file-based database as long as it is needed_
  - UUID generated for each query run
    - to easily get any executed query result
    - to work with one database from different scripts with no need to synchronization)
  - For select queries, result is saved in the form of pandas.DataFrame
  - Apart from the result, query meta information is also preserved (such as start and finish timestamps, duration)
  

## sql_asyncio 

**Note: will be fully documented in future releases**

Provides SqlAsyncExecutor for async query execution and SqlAsyncPlanner for running queries in background.


      from sqldbclient.sql_asyncio import SqlAsyncExecutor, SqlAsyncPlanner
      from sqlalchemy.ext.asyncio import create_async_engine

      async_engine = create_async_engine('postgresql+asyncpg://postgres:mysecretpassword@localhost:5555', pool_size=2)
      
      sql_async_executor = SqlAsyncExecutor(async_engine)
      df = await sql_async_executor.execute("SELECT 1 AS a")

      from datetime import datetime
      
      sql_async_planner = SqlAsyncPlanner(async_engine)

      start = datetime.now()
      sql_async_planner.put('SELECT pg_sleep(2)')
      sql_async_planner.put('SELECT pg_sleep(2)')
      sql_async_planner.put('SELECT pg_sleep(2)')
      await sql_async_planner.get()
      await sql_async_planner.get()
      await sql_async_planner.get()
      print(datetime.now() - start)


## dialects.postgresql

Helps to redefine view and materialized views without dropping any dependant objects manually.

**Note: will be fully documented in future releases**

**Warning: 'INSTEAD OF' view triggers are not supported yet 
and will not be automatically restored during view recreation**

    from sqldbclient.dialects.postgresql import SqlViewFactory, SqlViewMaterializer
    
    some_view = SqlViewFactory('view_name', 'view_schema', sql_executor).create()
    some_view.definition = '-- new definition'
    SqlViewMaterializer(some_view, sql_executor).materialize()


## DB Inspector

**Note: will be improved in future versions**

Provides a wrapper around sqlalchemy.inspect function.

Apart from standard sqlalchemy.engine.reflection.Inspector methods, the returned object has the following ones:
- print_columns

## Handy utils

- pandas.DataFrame full displaying in Jupyter Notebook: 
  - displays pandas.DataFrame with all rows and columns and full colwidth 
  - easy to use (just call a DataFrame method)
        
        import pandas as pd
        from sqldbclient.utils.pandas import full_display
        # now any pandas.DataFrame has method full_display available

        big_df = pd.read_csv(...)
        big_df.full_display(width=True)
- SqlEngineFactory
  - caches engines with the same parameters to prevent resources leakage

        from sqldbclient import sql_engine_factory
        
        # pass arguments to sqlalchemy.create_engine function
        engine = sql_engine_factory.get_or_create(*args, **kwargs)

