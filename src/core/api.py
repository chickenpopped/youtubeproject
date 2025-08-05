import re

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src import api_key

# Build youtube client
youtube = build("youtube", "v3", developerKey=api_key)


def camel_to_snake(name):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


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
        request_params["videoCategoryId"] = (
            category_id  # This is the API param, not DB field
        )

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
                
                # Handle tags conversion - convert list to comma-separated string
                snippet = video.get("snippet", {})
                if "tags" in snippet and isinstance(snippet["tags"], list):
                    # Join tags with commas, truncate to 500 chars if needed
                    tags_string = ", ".join(snippet["tags"])
                    if len(tags_string) > 500:
                        # Truncate at last complete tag that fits within 500 chars
                        truncated = tags_string[:497]  # Leave room for "..."
                        last_comma = truncated.rfind(", ")
                        if last_comma > 0:
                            tags_string = truncated[:last_comma] + "..."
                        else:
                            tags_string = truncated + "..."
                    snippet["tags"] = tags_string
                
                for key in video:
                    # Convert keys to snake_case
                    if isinstance(video[key], dict):
                        subkeys = list(video[key].keys())
                        for subkey in subkeys:
                            snake_subkey = camel_to_snake(subkey)
                            if subkey != snake_subkey:
                                video[key][snake_subkey] = video[key].pop(subkey)

            # Get next page token
            next_token = response.get("nextPageToken")
            if next_token:
                request_params["pageToken"] = next_token
                request = youtube.videos().list(**request_params)
            else:
                # No more pages
                break
        for idx, video in enumerate(videos):
            video["rank"] = (
                idx + 1
            )  # Add rank to each video based on its position in the list
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
            # Rename 'id' to 'channel_id' for DB consistency
            channel["channel_id"] = channel.pop("id", None)
            for key in channel:
                # Convert keys to snake_case
                if isinstance(channel[key], dict):
                    subkeys = list(channel[key].keys())
                    for subkey in subkeys:
                        snake_subkey = camel_to_snake(subkey)
                        if subkey != snake_subkey:
                            channel[key][snake_subkey] = channel[key].pop(subkey)
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
