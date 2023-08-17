"""
.. note::
   This module is under development, and currently
   provides minimal functionality.

Provides a wrapper around sqlalchemy ``inspect`` function.

Apart from standard ``sqlalchemy.engine.reflection.Inspector`` methods,
a ``DBInspector`` instance has the following functionalities:

   - creates text representation of table columns
   - provides get_views method to get consistent result throughout different sqlalchemy versions

"""

from sqldbclient.db_inspector.db_inspector import inspect
