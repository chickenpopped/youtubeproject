from datetime import datetime

import isodate
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import Session, sessionmaker

# Load environment variables from source
from src import database_url
from src.database.models import ChannelHistory, Channels, VideoData, VideoHistory

engine = create_engine(database_url, echo=True)

# Create session
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def calc_diff(current, previous):
    """
    Calculate the difference between current and previous values safely.
    Returns None if either value is None.
    """
    if current is None or previous is None:
        return None
    elif isinstance(current, datetime) or isinstance(previous, datetime):
        time_diff = current - previous
        return time_diff.total_seconds() / (24 * 3600)
    return current - previous


def growth_per_day(delta, days):
    """
    Calculate growth rate per day.
    Returns None if delta or days is None, or if days is 0.
    """
    if delta is None or days is None or days == 0:
        return None

    return delta / days


def move_old_data(session: Session) -> None:
    """
    Move old data to history tables.
    """
    try:
        current_videos = session.query(VideoData).all()
        current_channels = session.query(Channels).all()

        if not current_videos or not current_channels:
            print("No data to move")
            return

        video_ids = [v.video_id for v in current_videos]
        channel_ids = [c.channel_id for c in current_channels]

        # Query history tables

        prev_vid_histories = (
            session.query(VideoHistory)
            .filter(VideoHistory.video_id.in_(video_ids))
            .order_by(VideoHistory.video_id, VideoHistory.scraped_at.desc())
            .all()
        )
        prev_map_vid = {h.video_id: h for h in prev_vid_histories}

        prev_channel_histories = (
            session.query(ChannelHistory)
            .filter(ChannelHistory.channel_id.in_(channel_ids))
            .order_by(ChannelHistory.channel_id, ChannelHistory.scraped_at.desc())
            .all()
        )
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
            "scrape_category",
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
            "average_likes",
            "comment_count",
            "average_comments",
            "subscriber_count",
            "video_count",
            "popular_count",
        ]
        channel_delta_fields = [
            "view_count",
            "popular_view_count",
            "average_views",
            "like_count",
            "average_likes",
            "comment_count",
            "average_comments",
            "subscriber_count",
            "video_count",
            "popular_count",
        ]

        v_history = []
        for video in current_videos:
            prev = prev_map_vid.get(video.video_id)
            entry = {field: getattr(video, field) for field in video_fields}
            prev_scrape = prev.scraped_at if prev else None
            day_diff = calc_diff(video.scraped_at, prev_scrape)
            entry["days_since_scrape"] = day_diff
            for field in video_delta_fields:
                prev_val = getattr(prev, field) if prev else None
                delta = calc_diff(entry[field], prev_val)
                entry[f"{field}_delta"] = delta
                growth = growth_per_day(delta, day_diff)
                if field.endswith('_count'):
                    entry[f"{field.replace('_count', '')}_growth_per_day"] = growth
            v_history.append(entry)
        c_history = []
        for channel in current_channels:
            prev = prev_map_channel.get(channel.channel_id)
            entry = {field: getattr(channel, field) for field in channel_fields}
            prev_scrape = prev.scraped_at if prev else None
            day_diff = calc_diff(channel.scraped_at, prev_scrape)
            entry["days_since_scrape"] = day_diff
            for field in channel_delta_fields:
                prev_val = getattr(prev, field) if prev else None
                delta = calc_diff(entry[field], prev_val)
                entry[f"{field}_delta"] = delta
                growth = growth_per_day(delta, day_diff)
                entry[f"{field.replace('_count', '')}_growth_per_day"] = growth
                if field.endswith('_count'):
                    entry[f"{field.replace('_count', '')}_growth_per_day"] = growth
                elif field.startswith('average_'):
                    entry[f"{field[:-1]}_growth_per_day"] = growth
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
        raise


def add_record(item, table_class, session: Session, table_cols) -> None:
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
                            setattr(
                                new_row,
                                sub_key,
                                isodate.parse_duration(item[key][sub_key]),
                            )
                        elif sub_key == "tags":
                            print(f"DEBUG: Setting nested tags: {item[key][sub_key]}")
                            setattr(new_row, sub_key, item[key][sub_key])
                        else:
                            setattr(new_row, sub_key, item[key][sub_key])
    # Add to session
    session.add(new_row)


def ingest_table(data_list, table_class, session: Session) -> None:
    """
    Given a list of data, the target table, and current SQLAlchemy session,
    this function will ingest the data into the database.
    """
    # Get list of attributes for the table
    try:
        table_cols = {col.name for col in table_class.__table__.columns}

        for item in data_list:
            p_k_fields = [col.name for col in table_class.__table__.primary_key.columns]
            p_k_args = {field: item.get(field) for field in p_k_fields}
            add_record(item, table_class, session, table_cols)
            print(
                f"Added new {table_class.__name__} record with {p_k_fields} : {p_k_args}."
            )
    except Exception as e:
        session.rollback()
        print(f"Error ingesting to table: {e}")
        raise
