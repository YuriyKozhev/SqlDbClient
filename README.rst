Sql DB Client
=============

|packageversion|_
|docs|_
|totaldownloads|_
|monthdownloads|_

.. docincludebegin

Description
-----------

**Sql DB Client** is a Python interface for interacting with a database.

Its main goal is to provide a Python-based alternative
to basic database client software applications
(e.g. `DBeaver <https://en.wikipedia.org/wiki/DBeaver>`_),
especially in terms of executing SQL queries.
This package mostly aims at SQL scripts executing
since other types of database related activities
(e.g. database navigation)
can be done more conveniently with specifically designed graphical UI.

Based on powerful Python packages such as sqlalchemy, pandas and sqlparse,
it provides easy-to-use interface for executing SQL code along with other
additional functionalities:

- keeping track of all executed queries, their execution information and results
- parsing SQL queries (e.g. automatically adding LIMIT clause to prevent memory overflow)
- performing transaction by simply using ``with`` operator

:mod:`sqldbclient` is especially helpful for data analysts and engineers
who are used to work with Python and its packages
inside Jupyter Notebook environment, since it's meant for an interactive use
with the goal of analyzing, visualizing and interpreting data.
Note that a SQL query result will be shown and saved as a pandas ``DataFrame`` object.

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

Features
========

Here are some modules one most likely will use in their program.

sql_executor
------------

Main class, ``SqlExecutor``, inherits all functionalities from ``SqlHistoryManager``,
``SqlQueryPreparator`` and ``SqlTransactionManager``:

- ``SqlHistoryManager``
   - stores information about query executions and their results in local SQLite database
   - provides easy access to saved data via UUID
   - performs database cleaning to keep its size limited
- ``SqlQueryPreparator``
   - validates that there is exactly 1 statement in a query which is being executed
   - determines query type
   - formats query
   - automatically adds LIMIT clause to query
- ``SqlTransactionManager``
   - provides context manager for performing transactions

Moreover, ``SqlExecutor`` keeps configuration
(sqlalchemy engine parameters, default LIMIT clause value, file name for history database)
and provides single method for executing SQL queries.


sql_asyncio
-----------

.. note::
   The following tools are available only with sqlalchemy version >= **1.4**
   installed, since the support for asynchronous engines
   was added in that release.

``SqlAsyncExecutor`` is a simplified version of ``SqlExecutor``,
which provides a single method to execute queries asynchronously.
It may be useful for the case when one needs to execute queries in parallel or
to schedule an execution without blocking the main program.

``SqlQueryPreparator`` is a wrapper around ``SqlAsyncExecutor``
with builtin tasks queue, which is used to store and obtain results of
asynchronous executions. All queries are immediately scheduled for execution
once they are added to the queue.


db_inspector
------------

.. note::
   This module is under development, and currently
   provides minimal functionality.

Provides a wrapper around sqlalchemy ``inspect`` function.

Apart from standard ``sqlalchemy.engine.reflection.Inspector`` methods,
a ``DBInspector`` instance has the following functionalities:

   - creates text representation of table columns
   - provides get_views method to get consistent result throughout different sqlalchemy versions


dialects.postgresql
-------------------

``SqlViewFactory`` collects all available data about a regular
or materialized view and all its dependencies into a Python ``View`` object.
``SqlViewMaterializer`` applies changes made to a ``View`` instance to a corresponding database
object and all its dependencies.
Together, ``SqlViewFactory`` and ``SqlViewMaterializer`` provide
a tool which helps redefine a view in a database without
the need of manually dropping it and its dependencies and then recreating them all.
It also takes care of all the permissions that recreated objects had,
that is the permissions will be automatically restored along with the view
and its dependencies.
Note that all the necessary steps will be executed in a separate transaction,
which ensures that the whole operation either will be completed fully
or will not be done at all.


.. warning::
   *'INSTEAD OF'* view triggers are not supported yet
   and will not be automatically restored during view recreation.


utils
-----

Here are some helpful tools to:

- display pandas ``DataFrame`` in a full size (all rows and columns) in Jupyter Notebook environment

  .. code-block:: python

   import pandas as pd
   from sqldbclient import set_full_display

   set_full_display(max_rows=200, display_whole_colwidth_by_default=True)

   pd.DataFrame({'sample_column': range(150)}).full_display()


  .. note::
      By default, only ``DataFrame`` with the rows and columns numbers are less than
      **1000** can be displayed in full size.
      Otherwise, a corresponding exception is raised.


- grant access to a database object in a PostgreSQL database

  .. code-block:: python

   from sqldbclient.dialects.postgresql import grant_access

   pg_executor = SqlExecutor.builder.config(
      SqlExecutorConf().set('engine_options',
         'postgresql+psycopg2://postgres:mysecretpassword@localhost:5555')
   ).get_or_create()

   grant_access(
       object_name='sales_statistics',
       object_schema='public',
       user_name='postgres',
       sql_executor=pg_executor,
       privilege='SELECT',
   )


- create sqlalchemy engines and avoid resource leakage by keeping only one engine per a unique set of parameters

  .. code-block:: python

   from sqldbclient import sql_engine_factory

   # pass arguments and keyword arguments as to sqlalchemy create_engine function

   sqlite_engine = sql_engine_factory.get_or_create('sqlite:///my_sqlite.db')



Resources
---------

More information about available modules, classes and functions
can be found on `the documentation page <https://sqldbclient.readthedocs.io/>`_.

Project page
   https://github.com/YuriyKozhev/SqlDBClient

Bug tracker
   https://github.com/YuriyKozhev/SqlDBClient/issues

Documentation
   https://sqldbclient.readthedocs.io/

.. |packageversion| image:: https://img.shields.io/pypi/v/sqldbclient?color=lightgreen
.. _packageversion: https://pypi.org/project/sqldbclient
.. |docs| image:: https://readthedocs.org/projects/sqldbclient/badge/?version=latest
.. _docs: https://sqldbclient.readthedocs.io/en/latest/?badge=latest
.. |totaldownloads| image:: https://static.pepy.tech/badge/sqldbclient
.. _totaldownloads:  https://www.pepy.tech/projects/sqldbclient
.. |monthdownloads| image:: https://static.pepy.tech/badge/sqldbclient/month
.. _monthdownloads:  https://www.pepy.tech/projects/sqldbclient
