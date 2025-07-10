import isodate
from sqlalchemy import create_engine, insert, delete
from sqlalchemy.orm import sessionmaker

from src.models import VideoData, Channels, VideoHistory, ChannelHistory

# Load environment variables from source
from src import database_url

engine = create_engine(database_url, echo=True)

# Create session
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def move_old_video_data():
    """
    Move old data to history tables.
    """
    session = SessionLocal()
    try:
        current_videos = session.query(VideoData).all() # Query all current videos
        video_ids = [v.id for v in current_videos] # Get list of video IDs
        current_channels = session.query(Channels).all()
        channel_ids = [c.id for c in current_channels]  # Get list of channel IDs
        
        # Query VideoHistory table for all rows that have matching video ids
        prev_vid_histories = (
            session.query(VideoHistory)
            .filter(VideoHistory.video_id.in_(video_ids))
            .order_by(VideoHistory.video_id, VideoHistory.scraped_at.desc())
            .all()
        )
        prev_channel_histories = (
            session.query(ChannelHistory)
            .filter(ChannelHistory.channel_id.in_(channel_ids))
            .order_by(ChannelHistory.channel_id, ChannelHistory.scraped_at.desc())
            .all()
        )
        
        # Mapping of all previous histories by video_id
        prev_map_vid = {}
        for h in prev_vid_histories:
            if h.video_id not in prev_map_vid:
                prev_map_vid[h.video_id] = h
                
        prev_map_channel = {}
        for h in prev_channel_histories:
            if h.channel_id not in prev_map_channel:
                prev_map_channel[h.channel_id] = h
                
        v_history = [
            dict(
                video_id = video.id,
                scraped_at = video.scraped_at,
                title = video.title,
                description=video.description,
                published_at=video.published_at,
                view_count=video.view_count,
                view_delta = (video.view_count - prev_map_vid[video.id].view_count) if video.id in prev_map_vid else None,
                like_count = video.like_count,
                like_delta = (video.like_count - prev_map_vid[video.id].like_count) if video.id in prev_map_vid else None,
                comment_count = video.comment_count,
                comment_delta = (video.comment_count - prev_map_vid[video.id].comment_count) if video.id in prev_map_vid else None,
                duration = video.duration,
                tags = video.tags,
                rank = video.rank,
                scrape_type = video.scrape_type,
                channel_id = video.channel_id,
                category_id = video.category_id
            )
            for video in current_videos
        ]
        c_history = [
            dict(
                channel_id = channel.id,
                scraped_at = channel.scraped_at,
                title = channel.title,
                description = channel.description,
                published_at = channel.published_at,
                view_count = channel.view_count,
                view_delta = (channel.view_count - prev_map_channel[channel.id].view_count) if channel.id in prev_map_channel else None,
                subscriber_count = channel.subscriber_count,
                subscriber_delta = (channel.subscriber_count - prev_map_channel[channel.id].subscriber_count) if channel.id in prev_map_channel else None,
                video_count = channel.video_count,
                video_delta = (channel.video_count - prev_map_channel[channel.id].video_count) if channel.id in prev_map_channel else None
            )
            for channel in current_channels
        ]
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
            p_k_fields = [col.name for col in table_class.__table__.primary_key.columns]  # Get primary key field names
            p_k_args = {field: item.get(field) for field in p_k_fields}  # Get primary key arguments from item
            add_record(item, table_class, session, table_cols)
            print(f"Added new {table_class.__name__} record with {p_k_fields} : {p_k_args}.")
        # Commit session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error ingesting data: {e}")
    finally:
        session.close()
        print("Session closed.")
