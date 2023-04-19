from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker, Session


def build_session(
        engine: Engine = create_engine('postgresql://postgres:1234@localhost:5431/Chinook')
) -> Session:
    return sessionmaker(
        bind=engine
    )()
