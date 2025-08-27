import enum
from sqlalchemy import Enum
from typing import Optional, List
from sqlmodel import SQLModel, Field, ARRAY, String, Column
from datetime import datetime, timedelta


class VideoType(enum.Enum):
    popular = "popular"
    category = "category"


class VideoData(SQLModel, table=True):
    __tablename__: str = 'video_data'
    pk_id: Optional[int] = Field(default=None, primary_key=True)
    video_id: str = Field(index=True)
    title: str
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    description: Optional[str] = None
    published_at: datetime
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
    duration: Optional[timedelta] = None
    tags: Optional[list[str]] = Field(default_factory=list, sa_column=Column(ARRAY(String))) # current workaround for pydantic/sqlmodel list type validation
    scrape_type: VideoType = Field(sa_column=Column(Enum(VideoType)))
    scrape_category: Optional[int] = None
    rank: int
    channel_id: Optional[str] = Field(default=None, foreign_key="channels.channel_id")
    category_id: Optional[int] = Field(default=None, foreign_key="categories.category_id")
    
    class Config:
        arbitrary_types_allowed = True


# Table for historical video data
class VideoHistory(SQLModel, table=True):
    __tablename__: str = "video_history"
    id: Optional[int] = Field(default=None, primary_key=True)
    video_id: str
    scraped_at: datetime
    title: str
    description: Optional[str] = None
    published_at: datetime
    view_count: Optional[int] = None
    view_count_delta: Optional[int] = 0
    like_count: Optional[int] = None
    like_count_delta: Optional[int] = 0
    comment_count: Optional[int] = None
    comment_count_delta: Optional[int] = 0
    duration: Optional[timedelta] = None
    tags: Optional[list[str]] = Field(default_factory=list, sa_column=Column(ARRAY(String)))
    rank: int
    scrape_type: VideoType = Field(sa_column=Column(Enum(VideoType)))
    scrape_category: Optional[int] = None
    channel_id: str
    category_id: Optional[int] = None
    days_since_scrape: Optional[float] = None
    view_growth_per_day: Optional[float] = None
    like_growth_per_day: Optional[float] = None
    comment_growth_per_day: Optional[float] = None


class Categories(SQLModel, table=True):
    category_id: int = Field(primary_key=True, index=True)
    name: str
    assignable: bool


class Channels(SQLModel, table=True):
    # All statistics except view_count are for the channel's popular videos statistics
    channel_id: str = Field(primary_key=True, index=True)
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    title: str
    description: Optional[str] = None
    published_at: datetime
    view_count: Optional[int] = None
    popular_view_count: Optional[int] = None
    average_views: Optional[int] = None
    subscriber_count: Optional[int] = None
    video_count: Optional[int] = None
    popular_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
    average_likes: Optional[int] = None
    average_comments: Optional[int] = None


class ChannelHistory(SQLModel, table=True):
    __tablename__: str = "channel_history"
    id: Optional[int] = Field(default=None, primary_key=True)
    channel_id: str
    scraped_at: datetime
    title: str
    description: Optional[str] = None
    published_at: datetime
    view_count: Optional[int] = None
    view_count_delta: Optional[int] = None
    popular_view_count: Optional[int] = None
    popular_view_count_delta: Optional[int] = None
    average_views: Optional[int] = None
    average_views_delta: Optional[int] = None
    like_count: Optional[int] = None
    like_count_delta: Optional[int] = None
    average_likes: Optional[int] = None
    average_likes_delta: Optional[int] = None
    comment_count: Optional[int] = None
    comment_count_delta: Optional[int] = None
    average_comments: Optional[int] = None
    average_comments_delta: Optional[int] = None
    subscriber_count: Optional[int] = None
    subscriber_count_delta: Optional[int] = None
    video_count: Optional[int] = None
    video_count_delta: Optional[int] = None
    popular_count: Optional[int] = None
    popular_count_delta: Optional[int] = None
    days_since_scrape: Optional[float] = None
    view_growth_per_day: Optional[float] = None
    popular_view_growth_per_day: Optional[float] = None
    average_view_growth_per_day: Optional[float] = None
    like_growth_per_day: Optional[float] = None
    average_like_growth_per_day: Optional[float] = None
    comment_growth_per_day: Optional[float] = None
    average_comment_growth_per_day: Optional[float] = None
    subscriber_growth_per_day: Optional[float] = None
    video_growth_per_day: Optional[float] = None
    popular_count_growth_per_day: Optional[float] = None
