from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.orm import Session

from src.dao.album import AlbumDao


class DAOFacade:
    def __init__(self, base: AutomapBase, session: Session):
        self.album = AlbumDao(base=base, session=session)


