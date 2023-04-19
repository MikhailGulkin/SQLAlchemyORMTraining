from sqlalchemy import text, create_engine, MetaData, inspect, select
from sqlalchemy.ext.automap import automap_base

from src.config.db_setup import build_session
from src.dao.dao import DAOFacade


def main():
    engine = create_engine('postgresql://postgres:1234@localhost:5431/Chinook')
    session = build_session(engine=engine)
    metadata = MetaData()
    metadata.reflect(engine)

    Base = automap_base(metadata=metadata)

    Base.prepare()
    res = DAOFacade(base=Base, session=session).album.get_album_by_id_with_tacks(1)
    # print(res[0].__dict__)
    # print(res[1].__dict__)
    print(res)
    # for c in res:
    #     album, track = c
    #     print(album.__dict__, track.__dict__)

if __name__ == '__main__':
    main()
