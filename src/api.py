from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src import api_key

# Build youtube client
youtube = build("youtube", "v3", developerKey=api_key)


def scrape_data(category_id=None):
    """
    Scrape popular videos from Youtube API and return as Python list of dictionaries
    """
    videos = []
    channel_ids = set()

    # Initialize API request filters
    request_params = {
        "part": "snippet, statistics, contentDetails",
        "chart": "mostPopular",
        "regionCode": "US",
        "maxResults": 50,
    }

    # If a category ID is provided, add it to the request parameters
    if category_id:
        request_params["videoCategoryId"] = category_id

    try:
        # Handle the API request
        request = youtube.videos().list(**request_params)
        # Loop through the pages of results
        while request:
            response = request.execute()
            items = response.get("items", [])
            videos.extend(items)

            # Extract channel IDs to handle Channel table
            for video in items:
                channel_id = video.get("snippet", {}).get("channelId")
                if channel_id:
                    channel_ids.add(channel_id)

            # Get next page token
            next_token = response.get("nextPageToken")
            if next_token:
                request_params["pageToken"] = next_token
                request = youtube.videos().list(**request_params)
            else:
                # No more pages
                break
    # Some categories do not return any videos under the mostPopular chart, but still have videos assigned to them, returning 404
    except HttpError as e:
        print(f"HTTP error {e.resp.status}: {e.content}")
        return [], []
    except Exception as e:
        print(f"Error fetching data for category {category_id}: {e}")
        return [], []

    return videos, list(channel_ids)


def get_channel_data(channel_ids):
    """
    Fetch channel data from Youtube API and return as Python list of dictionaries
    """
    channels = []

    # YouTube API allows a maximum of 50 IDs per request
    batch_size = 50
    for i in range(0, len(channel_ids), batch_size):
        # Get the current batch of up to 50 channel IDs
        batch = channel_ids[i : i + batch_size]

        # Make the API request
        request = youtube.channels().list(part="snippet,statistics", id=batch)
        response = request.execute()

        # Add the channels from this batch to the result
        for channel in response.get("items", []):
            # Rename 'id' to 'channelId'
            channel["channelId"] = channel.pop("id", None)
            channels.append(channel)

    return channels


def get_video_categories():
    """
    Fetch video categories from Youtube API and return as Python list of dictionaries
    """
    request = youtube.videoCategories().list(part="snippet", regionCode="US")
    response = request.execute()
    categories = response.get("items", [])
    return categories
