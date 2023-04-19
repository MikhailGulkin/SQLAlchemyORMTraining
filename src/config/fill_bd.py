from sqlalchemy import text

from src.db_setup import build_session


def fill():
    session = build_session()
    with open('../Chinook/Chinook.sql') as file:
        session.execute(
            text(
                file.read()
            )
        )
        session.commit()


if __name__ == '__main__':
    fill()
