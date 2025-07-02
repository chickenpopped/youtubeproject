import isodate
from pyparsing import col
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
                video_id, scraped_at, title, description, published_at, view_count, like_count, comment_count, duration, tags, channel_id, category_id
            )
            SELECT id as video_id, scraped_at, title, description, published_at, view_count, like_count, comment_count, duration, tags, channel_id, category_id
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


def ingest_table(data_list, table_class, session):
    """
    Given a list of data, the target table, and current SQLAlchemy session,
    this function will ingest the data into the database.
    """
    try:
        # Get list of attributes for the table
        table_cols = {col.name for col in table_class.__table__.columns}

        for item in data_list:
            # Check if record exists in table
            p_k_fields = [col.name for col in table_class.__table__.primary_key.columns]  # Get primary key field names
            p_k_args = {field: item.get(field) for field in p_k_fields}  # Get primary key arguments from item
            existing_record = (
                session.query(table_class).filter_by(**p_k_args).first()
            )
            # If record exists in video data table, do not add it again
            if table_class.__name__ == "VideoData" and existing_record:
                print(f"Record already exists in {table_class.__name__} table with id {p_k_args}. Skipping.")
                continue
            # If record exists in channel table, update it
            if table_class.__name__ == "Channels" and existing_record:
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
                add_record(item, table_class, session, table_cols)
                print(f"Added new {table_class.__name__} record with id {p_k_args}.")
                continue
        # Commit session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error ingesting data: {e}")
    finally:
        session.close()
        print("Session closed.")
