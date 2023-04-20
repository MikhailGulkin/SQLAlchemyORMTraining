from sqlalchemy import Engine, create_engine as create_engine_
from sqlalchemy.orm import sessionmaker, Session


def create_engine() -> Engine:
    return create_engine_('postgresql://postgres:1234@localhost:5431/Northwind')


def build_session(
        engine: Engine = create_engine()
) -> Session:
    return sessionmaker(
        bind=engine
    )()
