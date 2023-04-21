from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base

from src.config.db_setup import build_session, create_engine
from src.sql.sql_query_northwind import SQLQueryNorthwind


def main():
    engine = create_engine()
    session = build_session(engine=engine)
    metadata = MetaData()
    metadata.reflect(engine)
    Base = automap_base(metadata=metadata)

    Base.prepare()

    sql_obj = SQLQueryNorthwind(base=Base, session=session)

    res = sql_obj.current_product_list()

    print(len(res))
    print(res, end='\n\n')
    for line in res[0:10]:
        print(line)


if __name__ == '__main__':
    main()