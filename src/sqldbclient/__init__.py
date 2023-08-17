__docformat__ = "restructuredtext"

# module level doc-string
__doc__ = """
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

"""

__version__ = '0.1.0'

import logging

from sqldbclient.sql_history_manager import SqlHistoryManager
from sqldbclient.sql_query_preparator import SqlQueryPreparator
from sqldbclient.sql_transaction_manager import SqlTransactionManager

from sqldbclient.sql_executor import SqlExecutor, SqlExecutorConf

from sqldbclient.sql_engine_factory import sql_engine_factory

try:
    from sqldbclient.utils.pandas.set_full_display import set_full_display
except ImportError:
    pass


logging.getLogger(__name__)
