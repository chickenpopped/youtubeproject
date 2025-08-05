from sqlalchemy import distinct, func, select, update

from src.core.api import get_channel_data, scrape_data
from src.database.database import SessionLocal, ingest_table, move_old_data
from src.database.models import Categories, Channels, VideoData, VideoType


def ingest_data():
    """
    Ingest data from the YouTube API into the database.
    """
    session = SessionLocal()
    try:
        move_old_data(session)

        categories = session.query(Categories).all()

        # Scrape popular videos in each category
        cat_videos, channel_ids = [], set()
        for category in categories:
            print(f"Scraping category: {category.category_id}")
            if not category.assignable:
                continue
            cat_video_data, cat_channel_data = scrape_data(category.category_id)
            for video in cat_video_data:
                video["scrape_type"] = (
                    VideoType.category
                )  # Set scrape type for category videos
                video["scrape_category"] = category.category_id
            cat_videos.extend(cat_video_data)
            channel_ids.update(cat_channel_data)
        # Scrape popular videos in general
        pop_videos, pop_channel_ids = scrape_data()
        for video in pop_videos:
            video["scrape_type"] = VideoType.popular  # Set scrape type for popular videos
            video["scrape_category"] = None  # No category for general popular videos

        channel_ids.update(pop_channel_ids)

        videos = cat_videos + pop_videos

        # Deduplicate videos based on unique constraints
        seen_videos = set()
        unique_videos = []
        for video in videos:
            if (video["id"], video["scrape_type"], video["scrape_category"]) not in seen_videos:
                seen_videos.add((video["id"], video["scrape_type"], video["scrape_category"]))
                video["video_id"] = video.pop("id")  # Rename id to video_id
                unique_videos.append(video)

        print(
            f"Scraped {len(channel_ids)} channels, {len(cat_videos)} category videos, {len(pop_videos)} popular videos."
        )

        channels = get_channel_data(channel_ids)
        
        # Deduplicate channels by channel_id to prevent primary key violations
        seen_channels = set()
        unique_channels = []
        for channel in channels:
            if channel["channel_id"] not in seen_channels:
                seen_channels.add(channel["channel_id"])
                unique_channels.append(channel)
        
        # Ingest channel data
        ingest_table(unique_channels, Channels, session)
        print("Channels ingested successfully.")

        # Ingest videos
        ingest_table(unique_videos, VideoData, session)
        print("Videos ingested successfully.")

        # Insert channel averages and popular counts
        # Subquery for unique videos by unique id and channel id
        unique_videos = (
            select(
                VideoData.channel_id,
                VideoData.video_id,
                VideoData.like_count,
                VideoData.comment_count,
                VideoData.view_count,
            )
            .distinct(VideoData.channel_id, VideoData.video_id)
            .subquery()
        )

        total_likes = (
            select(func.sum(unique_videos.c.like_count))
            .where(unique_videos.c.channel_id == Channels.channel_id)
            .correlate(Channels)
            .scalar_subquery()
        )
        total_comments = (
            select(func.sum(unique_videos.c.comment_count))
            .where(unique_videos.c.channel_id == Channels.channel_id)
            .correlate(Channels)
            .scalar_subquery()
        )
        total_views = (
            select(func.sum(unique_videos.c.view_count))
            .where(unique_videos.c.channel_id == Channels.channel_id)
            .correlate(Channels)
            .scalar_subquery()
        )
        avg_views = (
            select(func.avg(unique_videos.c.view_count))
            .where(unique_videos.c.channel_id == Channels.channel_id)
            .correlate(Channels)
            .scalar_subquery()
        )
        avg_likes = (
            select(func.avg(unique_videos.c.like_count))
            .where(unique_videos.c.channel_id == Channels.channel_id)
            .correlate(Channels)
            .scalar_subquery()
        )
        avg_comments = (
            select(func.avg(unique_videos.c.comment_count))
            .where(unique_videos.c.channel_id == Channels.channel_id)
            .correlate(Channels)
            .scalar_subquery()
        )
        popular_count = (
            select(func.count(distinct(VideoData.video_id)))
            .where(VideoData.channel_id == Channels.channel_id)
            .correlate(Channels)
            .scalar_subquery()
        )

        # Update channel averages and popular counts
        session.execute(
            update(Channels)
            .values(
                like_count=total_likes,
                comment_count=total_comments,
                popular_view_count=total_views,
                average_views=avg_views,
                average_likes=avg_likes,
                average_comments=avg_comments,
                popular_count=popular_count,
            )
            .where(Channels.channel_id == VideoData.channel_id)
        )

        session.commit()
        print("Data ingested successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error ingesting data: {e}")
    finally:
        session.close()
        print("Session closed.")
