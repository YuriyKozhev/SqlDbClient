from dataclasses import dataclass, field

from sqlalchemy.sql.elements import TextClause
import sqlalchemy


@dataclass
class PreparedSqlQuery:
    text: str
    query_type: str
    nstatements: int
    text_sa_clause: TextClause = field(init=False)

    def __post_init__(self):
        self.text_sa_clause = sqlalchemy.text(self.text)
