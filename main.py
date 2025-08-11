import sys

from src.core.ingest_data import ingest_data
from src.database.init_db import init_db, reset_db


def main():
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "reset":
            reset_db()
        elif command == "init":
            init_db()
        elif command == "ingest":
            ingest_data()
        else:
            print("Invalid command. Use 'reset', 'init', or 'ingest'.")
    else:
        print("No command provided. Use 'reset', 'init', or 'ingest'.")
        ingest_data()


if __name__ == "__main__":
    main()
