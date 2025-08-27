# (Reset and) initialize the database and populate it with categories from the YouTube API.
from src.core.api import get_video_categories
from sqlmodel import SQLModel
from src.database.database import get_session, engine
from src.database.models import Categories


def reset_db():
    """
    Drop all tables in the database.
    """
    SQLModel.metadata.drop_all(bind=engine)
    print("Database reset")


def init_db():
    """
    Initialize the database and create tables.
    """
    # Create all tables in the database
    SQLModel.metadata.create_all(bind=engine)
    print("Database initialized and tables created.")

    session = get_session()

    # Populate categories table
    categories = get_video_categories()
    try:
        for category in categories:
            # Extract relevant data from the API response
            snippet = category.get("snippet", {})
            # Create a new Categories instance
            new_category = Categories(
                category_id=category.get("id"),
                name=snippet.get("title"),
                assignable=snippet.get("assignable"),
            )

            # Add category to session
            session.add(new_category)
        # Commit session
        session.commit()
        print("Categories populated successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error populating categories: {e}")
    finally:
        session.close()
        print("Session closed.")
