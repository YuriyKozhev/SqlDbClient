Sql DB Client
=============

|buildstatus|_
|coverage|_
|docs|_
|packageversion|_

.. docincludebegin

**Sql DB Client** is a client for executing SQL queries from Python.
It provides support for parsing, splitting and formatting SQL statements.

The module is compatible with Python 3.6+ and released under the terms of the
`MIT License <https://opensource.org/license/mit/>`_.

Visit the project page at https://github.com/YuriyKozhev/SqlDbClient for
further information about this project.


Quick Start
-----------

The latest released version of **Sql DB Client** can be obtained from the `Python Package
Index (PyPI) <https://pypi.org/project/sqlparse/>`_.
You can install :mod:`sqldbclient` using :command:`pip`:

.. code-block:: sh

   $ pip install sqldbclient

``SqlExecutor``, main tool to execute SQL queries,
is configured and created using ``SqlExecutorConf`` object.

Let's create *pg_executor*,
a new ``SqlExecutor`` instance for a PostgreSQL database.

.. code-block:: python

    from sqldbclient import SqlExecutor, SqlExecutorConf


    pg_executor = SqlExecutor.builder.config(
        SqlExecutorConf()
        # arguments to pass to sqlalchemy create_engine function
        .set('engine_options',
            # database connection string
            'postgresql+psycopg2://postgres:mysecretpassword@localhost:5555',
            # recycle connections after one hour
            pool_recycle=3600,
        ).set('history_db_name',
            # name of the SQLite database file that will be used
            # If the file exists,
            # it will used by SqlHistoryManager to store and load query results.
            # Otherwise, SQLite database with the corresponding file name will be created.
            'sql_executor_history.db'
        ).set('max_rows_read',
            # default value to be used in LIMIT clause, that will be added to SELECT queries
            10_000
        )
    # creates new instance of SqlExecutor with specified options,
    # or uses existing one in case it was created before
    ).get_or_create()

Now, let's create a new table using transaction.

With ``SqlExecutor`` instance,
it can be as easy as using its instance as a context manager.

.. code-block:: python

    # a new transaction is started by using a with statement
    with pg_executor:
        # multiple SQL statements can be executed in one transaction
        # to ensure that either every statement will take effect or none of them will
        pg_executor.execute('''
            DROP TABLE IF EXISTS sales_statistics
        ''')
        pg_executor.execute('''
            CREATE TABLE sales_statistics AS
                SELECT '2023-01-01'::date AS date_day, 5332 AS sales_total
            UNION ALL
                SELECT '2023-02-01'::date AS date_day, 8676 AS sales_total
            UNION ALL
                SELECT '2023-03-01'::date AS date_day, 1345 AS sales_total
        ''')
        # if assertion fails, the transaction will be rolled back
        assert (pg_executor.execute('''
            SELECT * FROM sales_statistics
        ''').sales_total > 0).all()
        # if there is no commit method call,
        # the transaction will be rolled back by default
        pg_executor.commit()

Finally, let's check out data from the source we have just created.

.. code-block:: python

    pg_executor.execute('''
        SELECT * FROM sales_statistics
    ''')

If the logging is set up to show warnings (by default), first we will see the following message

.. pull-quote::
    SELECT query will be limited to 10000

indicating that ``SqlExecutor`` automatically added LIMIT clause to the query.

The next message will be an ``ExecutedSqlQuery`` instance.

.. pull-quote::
    Executed ExecutedSqlQuery(uuid='88134b9cd6774d33b314003e21556d72', query='SELECT * FROM sales_statistics LIMIT 10000', start_time='2023-08-12 21:03:10', finish_time='2023-08-12 21:03:10', duration='0:00:00', query_type='SELECT')

After that, a Pandas ``DataFrame`` object will be displayed as an output.

====  ==========  =============
  ..  date_day      sales_total
====  ==========  =============
   0  2023-01-01           5332
   1  2023-02-01           8676
   2  2023-03-01           1345
====  ==========  =============

Then, we can reference the ``DataFrame`` object using the **UUID** assigned to ``ExecutedSqlQuery``
to calculate the overall sum for *sales_total* field.

.. code-block:: python

    >>> pg_executor['88134b9cd6774d33b314003e21556d72'].sales_total.sum()
    15353

By storing results of executed queries in a SQLite database, we assure
that they will be accessible after restarting the program,
or even can used in another Jupyter notebook
(as long as the SQLite database file is present in the same directory as a notebook).

Sql Executor
==============

Sql DB Client provides additional functionalities to work with DBMS utilizing powerful Python packages such as sqlalchemy and pandas.

The main goal to provide a handy alternative to basic SQL client software applications 

(e.g. `DBeaver](https://en.wikipedia.org/wiki/DBeaver), [pgAdmin <https://www.pgadmin.org/>`_, `pgAdmin <https://www.pgadmin.org/>`_, etc.).

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

# Sql Executor2
==============

Either can be build from a config

	  from sqldbclient import SqlExecutor, SqlExecutorConf

	  sql\_executor = SqlExecutor.builder.config(

		  SqlExecutorConf()

			  # pass arguments to sqlalchemy.create\_engine function

			  .set('engine\_options', 'postgresql+psycopg2://postgres:mysecretpassword@localhost:5555', echo=False)

			  # choose the name of sqlite database file with saved queries results

			  .set('history\_db\_name', 'sql\_executor\_history.db')

			  # set the maximum number of rows a SELECT query can fetch

			  .set('max\_rows\_read', 10\_000)

	  ).get\_or\_create()

Or created explicitly, but sqlalchemy Engine needs to created first (also you can use SqlEngineFactory for that)
	  
	  import sqlalchemy

	  from sqldbclient import SqlExecutor

	  sqlite\_engine = sqlalchemy.create\_engine('sqlite:///my\_sqlite.db')

	  sql\_executor = SqlExecutor(

		engine=sqlite\_engine, 

		max\_rows\_read=10\_000, 

		history\_db\_name='sql\_executor\_history.db'

	  )

Though it is recommended to build it from a config since 

then SqlExecutor instance and corresponding sqlalchemy engine will be automatically cached.

It will ensure no leakage of resources if one try to create multiple instances.


Roughly speaking, it is a wrapper over pd.DataFrame().read_sql method but with the following features:
- Automatic SELECT queries preprocessing and limiting to a configured number (to help avoiding memory overuse) - via SqlQueryPreparator
	  
	  '''SELECT \* from  

		  some\_table'

	  '''select \*    FROM some\_table

	   LIMIT too\_large\_limit'''

  The queries above will be transformed to the query below

	  if the limit is not specified or exceeds the configured number

	  '''SELECT \* 

		 FROM some\_table

		 LIMIT {max\_rows\_read}'''

- Easy transaction management (using context manager) - via SqlTransactionManager
		
		with sql\_executor:

			sql\_executor.execute('INSERT INTO some\_table VALUES (1, 2, 3)')

			sql\_executor.commit() #  otherwise the transaction will be automatically rolled back

- Query results storing in a SQLite database 
(i.e. a file inside a directory with your scripts)  - via SqlHistoryManager

  - _No need to save select results into csv and excel files in order to work with them in the future.

		They will be available in the file\-based database as long as it is needed\_

  - UUID generated for each query run

	\- to easily get any executed query result

	\- to work with one database from different scripts with no need to synchronization)

  - For select queries, result is saved in the form of pandas.DataFrame

  - Apart from the result, query meta information is also preserved (such as start and finish timestamps, duration)
  

# sql_asyncio
=============

**Note: will be fully documented in future releases**

Provides SqlAsyncExecutor for async query execution and SqlAsyncPlanner for running queries in background.


	  from sqldbclient.sql\_asyncio import SqlAsyncExecutor, SqlAsyncPlanner

	  from sqlalchemy.ext.asyncio import create\_async\_engine

	  async\_engine = create\_async\_engine('postgresql+asyncpg://postgres:mysecretpassword@localhost:5555', pool\_size=2)
	  
	  sql\_async\_executor = SqlAsyncExecutor(async\_engine)

	  df = await sql\_async\_executor.execute("SELECT 1 AS a")

	  from datetime import datetime
	  
	  sql\_async\_planner = SqlAsyncPlanner(async\_engine)

	  start = datetime.now()

	  sql\_async\_planner.put('SELECT pg\_sleep(2)')

	  sql\_async\_planner.put('SELECT pg\_sleep(2)')

	  sql\_async\_planner.put('SELECT pg\_sleep(2)')

	  await sql\_async\_planner.get()

	  await sql\_async\_planner.get()

	  await sql\_async\_planner.get()

	  print(datetime.now() \- start)


# dialects.postgresql
=====================

Helps to redefine view and materialized views without dropping any dependant objects manually.

**Note: will be fully documented in future releases**

**Warning: 'INSTEAD OF' view triggers are not supported yet 

and will not be automatically restored during view recreation**

	from sqldbclient.dialects.postgresql import SqlViewFactory, SqlViewMaterializer
	
	some\_view = SqlViewFactory('view\_name', 'view\_schema', sql\_executor).create()

	some\_view.definition = '\-\- new definition'

	SqlViewMaterializer(some\_view, sql\_executor).materialize()


# DB Inspector
==============

**Note: will be improved in future versions**

Provides a wrapper around sqlalchemy.inspect function.

Apart from standard sqlalchemy.engine.reflection.Inspector methods, the returned object has the following ones:
- print_columns

# Handy utils
=============

- pandas.DataFrame full displaying in Jupyter Notebook: 
  - displays pandas.DataFrame with all rows and columns and full colwidth 

  - easy to use (just call a DataFrame method)
		
		import pandas as pd

		from sqldbclient.utils.pandas import full\_display

		# now any pandas.DataFrame has method full\_display available

		big\_df = pd.read\_csv(...)

		big\_df.full\_display(width=True)
- SqlEngineFactory
  - caches engines with the same parameters to prevent resources leakage

		from sqldbclient import sql\_engine\_factory
		
		# pass arguments to sqlalchemy.create\_engine function

		engine = sql\_engine\_factory.get\_or\_create(\*args, \*\*kwargs)


