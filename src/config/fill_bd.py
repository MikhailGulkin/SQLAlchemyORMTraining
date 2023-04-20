from sqlalchemy import text

from src.config.db_setup import build_session


def fill():
    session = build_session()
    with open('../../Northwind/Northwind.sql') as file:
        session.execute(
            text(
                file.read()
            )
        )
        session.commit()


if __name__ == '__main__':
    fill()
