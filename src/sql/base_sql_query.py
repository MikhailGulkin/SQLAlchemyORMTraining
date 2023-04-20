from abc import ABC, abstractmethod

from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.orm import Session


class BaseSQLQuery(ABC):

    @abstractmethod
    def __init__(self, base: AutomapBase, session: Session) -> None:
        self.base = base
        self.session = session
