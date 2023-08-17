__docformat__ = "restructuredtext"

# module level doc-string
__doc__ = """
``SqlHistoryManager``
   - stores information about query executions and their results in local SQLite database
   - provides easy access to saved data via UUID
   - performs database cleaning to keep its size limited

"""

from sqldbclient.sql_history_manager.sql_history_manager import SqlHistoryManager
