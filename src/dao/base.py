from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.orm import Session


class BaseDao:
    def __init__(self, *, base: AutomapBase, session: Session):
        self.base = base
        self.session = session

