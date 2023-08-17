"""
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

"""

from sqldbclient.dialects.postgresql.sql_view_factory.sql_view_factory import SqlViewFactory
from sqldbclient.dialects.postgresql.sql_view_materializer.sql_view_materializer_utils import SqlViewMaterializerUtils
from sqldbclient.dialects.postgresql.sql_view_materializer.sql_view_materializer import SqlViewMaterializer

from sqldbclient.dialects.postgresql.utils import grant_access
