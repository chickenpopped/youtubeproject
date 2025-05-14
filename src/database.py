import isodate
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import text

# Load environment variables from source
from src import database_url

engine = create_engine(database_url, echo=True)

# Create session
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

# Base class
Base = declarative_base()


def move_old_video_data():
    """
    Move old video data to history table.
    """
    session = SessionLocal()
    try:
        session.execute(text("""DELETE FROM popular_videos;"""))
        # Move old data from popular_videos to video_history
        session.execute(
            text("""
            WITH moved_rows as (
            DELETE FROM category_videos
            RETURNING *
            )
            INSERT INTO video_history (
                "videoId", timestamp, title, description,"publishedAt", "viewCount", "likeCount", "commentCount", duration, tags, "channelId", "categoryId"
            )
            SELECT id as "videoId", timestamp, title, description, "publishedAt", "viewCount","likeCount", "commentCount", duration, tags, "channelId", "categoryId"
            FROM moved_rows
        """)
        )
        session.commit()
        print("Old video data moved to history table.")
    except Exception as e:
        session.rollback()
        print(f"Error moving old video data: {e}")
    finally:
        session.close()


def add_record(item, table_class, session, table_cols):
    """
    Add record to table.
    """
    new_row = table_class()
    for key in item:
        if key in table_cols:
            # Unnested attributes directly set
            setattr(new_row, key, item[key])
        # Handle nested attributes, nothing significant past second level
        else:
            if isinstance(item[key], dict):
                for sub_key in item[key]:
                    if sub_key in table_cols:
                        if sub_key == "duration":
                            # Convert duration to ISO 8601 format
                            setattr(
                                new_row,
                                sub_key,
                                isodate.parse_duration(item[key][sub_key]),
                            )
                        else:
                            setattr(new_row, sub_key, item[key][sub_key])
    # Add to session
    session.add(new_row)
    print(
        f"Added new {table_class.__name__} record with id {new_row.__table__.primary_key.columns[0]}."
    )


def ingest_table(data_list, table_class, session, update_existing=False):
    """
    Given a list of data, the target table, and current SQLAlchemy session,
    this function will ingest the data into the database.
    """
    try:
        # Get list of attributes for the table
        table_cols = {col.name for col in table_class.__table__.columns}

        for item in data_list:
            # Check if record exists in table
            p_k_field = list(table_class.__table__.primary_key.columns)[
                0
            ].name  # Get primary key field name
            p_k_arg = item.get(p_k_field)  # Get primary key argument from item
            existing_record = (
                session.query(table_class).filter_by(**{p_k_field: p_k_arg}).first()
            )

            from src.models import (  # Lazy import to prevent circular import
                CategoryVideos,
                PopularVideos,
            )

            # If record exists, update it
            if existing_record:
                # Update_existing should be true for channel logic only
                if update_existing:
                    snippet = item.get("snippet", {})
                    statistics = item.get("statistics", {})
                    # Update existing channel record
                    existing_record.name = snippet.get("title")
                    existing_record.description = snippet.get("description")
                    existing_record.view_count = statistics.get("viewCount")
                    existing_record.subscriber_count = statistics.get("subscriberCount")
                    existing_record.video_count = statistics.get("videoCount")
                    continue
                else:
                    continue
            # If target table is PopularVideos, check if video exists in CategoryVideos table
            elif table_class == PopularVideos:
                existing_cat_vid = (
                    session.query(CategoryVideos).filter_by(id=p_k_arg).first()
                )
                if not existing_cat_vid:
                    add_record(
                        item,
                        CategoryVideos,
                        session,
                        {
                            col.name for col in CategoryVideos.__table__.columns
                        },  # Get columns for CategoryVideos
                    )
                    print(f"Added new CategoryVideos record with id {p_k_arg}.")
                add_record(item, PopularVideos, session, "id")
                continue
            # Otherwise create new table class instance
            add_record(item, table_class, session, table_cols)
        # Commit session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error ingesting data: {e}")
    finally:
        session.close()
        print("Session closed.")
