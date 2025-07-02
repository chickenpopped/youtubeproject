from src.api import get_channel_data, scrape_data
from src.database import SessionLocal, ingest_table, move_old_video_data
from src.models import Categories, VideoData, Channels, VideoType


def ingest_data():
    """
    Ingest data from the YouTube API into the database.
    """
    # Move old video data to history table
    move_old_video_data()

    # Query Categories table for category IDs
    session = SessionLocal()
    categories = session.query(Categories).all()

    # Scrape popular videos in each category
    cat_videos, channel_ids = [], set()
    temp_vid = []
    dupe_videos = []
    for category in categories:
        print(f"Scraping category: {category.category_id}")
        # Check for assignability
        if not category.assignable:
            continue
        cat_video_data, cat_channel_data = scrape_data(category.category_id)            
        for video in cat_video_data:
            video["scrape_type"] = VideoType.category  # Set scrape type for category videos
            if (video["id"], video["scrape_type"], video["snippet"]["category_id"]) in temp_vid:
                dupe_videos.append((video["id"], video["scrape_type"], video["snippet"]["category_id"], video["rank"]))
                continue
            temp_vid.append((video["id"], video["scrape_type"], video["snippet"]["category_id"]))
        cat_videos.extend(cat_video_data)
        channel_ids.update(cat_channel_data)
    print(dupe_videos)
    # Scrape popular videos in general
    pop_videos, pop_channel_ids = scrape_data()
    for video in pop_videos:
        video["scrape_type"] = VideoType.popular # Set scrape type for popular videos
    channel_ids.update(pop_channel_ids)
    channel_ids = list(channel_ids)  # handle duplicate channels
    print(
        f"Scraped {len(channel_ids)} channels, {len(cat_videos)} category videos, {len(pop_videos)} popular videos."
    )

    # Ingest data into the database
    try:
        # Fetch channel data
        channels = get_channel_data(channel_ids)

        # Ingest channel data
        ingest_table(channels, Channels, session)
        print("Channels ingested successfully.")

        # Ingest category videos
        ingest_table(cat_videos, VideoData, session)
        print("Category videos ingested successfully.")

        # Ingest general popular videos
        ingest_table(pop_videos, VideoData, session)
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
