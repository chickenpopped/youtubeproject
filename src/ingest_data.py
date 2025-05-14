from datetime import datetime

from src.api import get_channel_data, scrape_data
from src.database import SessionLocal, ingest_table, move_old_video_data
from src.models import Categories, CategoryVideos, Channels, PopularVideos


def ingest_data():
    """
    Ingest data from the YouTube API into the database.
    """
    # Move old video data to history table
    move_old_video_data()

    # Query Categories table for category IDs
    session = SessionLocal()
    current_timestamp = datetime.now().isoformat()
    categories = session.query(Categories).all()

    # Scrape popular videos in each category
    cat_videos, channel_ids = (
        [],
        set(),
    )  # We want channel ids across both video sets to be unified

    for category in categories:
        print(f"Scraping category: {category.categoryId}")
        # Check for assignability
        if not category.assignable:
            continue
        cat_video_data, cat_channel_data = scrape_data(category.categoryId)
        cat_videos.extend(cat_video_data)
        channel_ids.update(cat_channel_data)

    # Scrape popular videos in general
    pop_videos, pop_channel_ids = scrape_data()
    channel_ids.update(pop_channel_ids)
    channel_ids = list(channel_ids)  # handle duplicate channels
    print(
        f"Scraped {len(channel_ids)} channels, {len(cat_videos)} category videos, {len(pop_videos)} popular videos."
    )
    # Add current timestamp to each video
    for vid in cat_videos:
        vid["timestamp"] = current_timestamp
    for vid in pop_videos:
        vid["timestamp"] = current_timestamp

    # Ingest data into the database
    try:
        # Fetch channel data
        channels = get_channel_data(channel_ids)

        # Ingest channel data
        ingest_table(channels, Channels, session, update_existing=True)
        print("Channels ingested successfully.")

        # Ingest category videos
        ingest_table(cat_videos, CategoryVideos, session)
        print("Category videos ingested successfully.")

        # Ingest general popular videos
        ingest_table(pop_videos, PopularVideos, session)
        print("Popular videos ingested successfully.")

        # Commit session
        session.commit()
        print("Data ingested successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error ingesting data: {e}")
    finally:
        session.close()
        print("Session closed.")
