from sqlalchemy import text, create_engine, MetaData, inspect, select
from sqlalchemy.ext.automap import automap_base

from src.config.db_setup import build_session
from src.sql.sql_query import SQLQueryChinook


def main():
    engine = create_engine('postgresql://postgres:1234@localhost:5431/Chinook')
    session = build_session(engine=engine)
    metadata = MetaData()
    metadata.reflect(engine)
    Base = automap_base(metadata=metadata)

    Base.prepare()

    sql_obj = SQLQueryChinook(base=Base, session=session)

    res = sql_obj.top_media_type()

    # print(len(res))
    # print(res)
    # for line in res:
    #     print(line)


if __name__ == '__main__':
    main()
