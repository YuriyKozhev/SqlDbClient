__docformat__ = "restructuredtext"

# module level doc-string
__doc__ = """
sql-executor 
=====================================================================

**sql-executor** provides additional functionalities to work with DBMS utilizing 
mainly 2 powerful packages sqlalchemy and pandas.


Main Features
-------------
Here are just a few of the things that sql-executor can do:

  - to be written...
"""

__version__ = '0.1.0'

import logging


from sqldbclient.sql_executor import SqlExecutor, SqlExecutorConf
from sqldbclient.sql_engine_factory import sql_engine_factory

from sqldbclient.utils.pandas.set_full_display import set_full_display


logging.getLogger(__name__)
