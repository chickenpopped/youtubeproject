from src.ingest_data import ingest_data
from src.init_db import init_db, reset_db


def main():
    reset_db()
    init_db()
    ingest_data()


if __name__ == "__main__":
    main()
