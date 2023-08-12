Introduction
============


Download & Installation
-----------------------

The latest released version of **Sql DB Client** can be obtained from the `Python Package
Index (PyPI) <https://pypi.org/project/sqlparse/>`_.
You can install :mod:`sqldbclient` using :command:`pip`:

.. code-block:: sh

   $ pip install sqldbclient


Getting Started
---------------


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


Development & Contributing
--------------------------

To check out the latest sources of this module run

.. code-block:: bash

   $ git clone git://github.com/YuriyKozhev/SqlDbClient.git


to check out the latest sources from the repository.

:mod:`sqldbclient` is currently tested under Python 3.6+.


Please file bug reports and feature requests on the project site at
https://github.com/YuriyKozhev/SqlDbClient/issues/new.