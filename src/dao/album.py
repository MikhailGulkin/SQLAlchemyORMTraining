from sqlalchemy import select
from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.orm import Session

from src.dao.base import BaseDao


class AlbumDao(BaseDao):
    def __init__(self, *, base: AutomapBase, session: Session):
        super().__init__(base=base, session=session)
        self.album = self.base.classes['Album']

    def get_all_album(self):
        query = select(self.album)
        return self.session.execute(query).fetchall()

    def get_album_by_id_with_tacks(self, id_: int):
        tracks = self.base.classes['Track']
        query = select(self.album.Title, tracks.Name). \
            join(tracks). \
            where(self.album.AlbumId == id_)
        return self.session.execute(query).fetchall()
