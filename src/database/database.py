import isodate
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

# Load environment variables from source
from src import database_url
from src.database.models import ChannelHistory, Channels, VideoData, VideoHistory

engine = create_engine(database_url, echo=True)

# Create session
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def safe_delta(current, previous):
    """
    Calculate the delta between current and previous values safely.
    Returns None if either value is None.
    """
    if current is None or previous is None:
        return None
    return current - previous


def move_old_videos():
    """
    Move old data to history tables.
    """
    session = SessionLocal()
    try:
        current_videos = session.query(VideoData).all()  # Query all current videos
        video_ids = [v.video_id for v in current_videos]  # Get list of video IDs
        current_channels = session.query(Channels).all()
        channel_ids = [
            c.channel_id for c in current_channels
        ]  # Get list of channel IDs

        # Query history tables for all rows that have matching ids
        prev_vid_histories = (
            session.query(VideoHistory)
            .filter(VideoHistory.video_id.in_(video_ids))
            .order_by(VideoHistory.video_id, VideoHistory.scraped_at.asc())
            .all()
        )
        prev_channel_histories = (
            session.query(ChannelHistory)
            .filter(ChannelHistory.channel_id.in_(channel_ids))
            .order_by(ChannelHistory.channel_id, ChannelHistory.scraped_at.asc())
            .all()
        )

        # Mapping of any scraped to their previous history, if it exists
        # Only keeps most recent history, from ascending order
        prev_map_vid = {h.video_id: h for h in prev_vid_histories}
        prev_map_channel = {h.channel_id: h for h in prev_channel_histories}

        video_fields = [
            "video_id",
            "scraped_at",
            "title",
            "description",
            "published_at",
            "view_count",
            "like_count",
            "comment_count",
            "duration",
            "tags",
            "rank",
            "scrape_type",
            "channel_id",
            "category_id",
        ]
        video_delta_fields = [
            "view_count",
            "like_count",
            "comment_count",
        ]  # Field for deltas

        channel_fields = [
            "channel_id",
            "scraped_at",
            "title",
            "description",
            "published_at",
            "view_count",
            "popular_view_count",
            "average_views",
            "like_count",
            "comment_count",
            "average_comments",
            "subscriber_count",
            "video_count",
        ]
        channel_delta_fields = [
            "view_count",
            "popular_view_count",
            "average_views",
            "like_count",
            "comment_count",
            "average_comments",
            "subscriber_count",
            "video_count",
        ]

        v_history = []
        for video in current_videos:
            prev = prev_map_vid.get(
                video.video_id
            )  # Previous video history, if it exists
            entry = {
                field: getattr(video, field) for field in video_fields
            }  # dictionary of current video's keys and values
            for field in video_delta_fields:
                prev_val = getattr(prev, field) if prev else None
                entry[f"{field}_delta"] = safe_delta(
                    entry[field], prev_val
                )  # Calculate delta if possible
            v_history.append(entry)
        c_history = []
        for channel in current_channels:
            prev = prev_map_channel.get(channel.channel_id)
            entry = {field: getattr(channel, field) for field in channel_fields}
            for field in channel_delta_fields:
                prev_val = getattr(prev, field) if prev else None
                entry[f"{field}_delta"] = safe_delta(entry[field], prev_val)
            c_history.append(entry)

        session.execute(insert(VideoHistory), v_history)
        session.execute(insert(ChannelHistory), c_history)
        session.execute(delete(VideoData))
        session.execute(delete(Channels))
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


def ingest_table(data_list, table_class, session):
    """
    Given a list of data, the target table, and current SQLAlchemy session,
    this function will ingest the data into the database.
    """
    try:
        # Get list of attributes for the table
        table_cols = {col.name for col in table_class.__table__.columns}

        for item in data_list:
            p_k_fields = [
                col.name for col in table_class.__table__.primary_key.columns
            ]  # Get primary key field names
            p_k_args = {
                field: item.get(field) for field in p_k_fields
            }  # Get primary key arguments from item
            add_record(item, table_class, session, table_cols)
            print(
                f"Added new {table_class.__name__} record with {p_k_fields} : {p_k_args}."
            )
        # Commit session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error ingesting data: {e}")
    finally:
        session.close()
        print("Session closed.")
