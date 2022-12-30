__docformat__ = "restructuredtext"

# module level doc-string
__doc__ = """
sql-executor 
=====================================================================

**sql-executor ** provides additional functionalities to work with DBMS utilizing 
mainly 2 powerful packages sqlalchemy and pandas.


Main Features
-------------
Here are just a few of the things that sql-executor can do:

  - to be written...
"""

import logging


from sqldbclient.sql_executor import SqlExecutor, SqlExecutorConf
from sqldbclient.utils import pandas as pandas_tools
from sqldbclient import db_inspector


logging.getLogger(__name__)
