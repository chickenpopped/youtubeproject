# This file defines the SQLAlchemy models for the YouTube video data and categories.

import enum

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    Interval,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship

from src.database.base import Base


class VideoType(enum.Enum):
    popular = "popular"
    category = "category"


class VideoData(Base):
    __tablename__ = "video_data"

    pk_id = Column(
        Integer, primary_key=True, autoincrement=True
    )  # Primary key for the table
    video_id = Column(String(255), index=True)  # Id of the video
    title = Column(String(255), nullable=False)
    scraped_at = Column(DateTime, nullable=False, server_default=func.now())
    description = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=False)
    view_count = Column(BigInteger, nullable=True)
    like_count = Column(BigInteger, nullable=True)
    comment_count = Column(Integer, nullable=True)
    duration = Column(Interval, nullable=True)  # Stored as ISO8601 duration format
    tags = Column(ARRAY(String), nullable=True)
    scrape_type = Column(Enum(VideoType), nullable=False)  # Column for scrape type
    scrape_category = Column(
        Integer, nullable=True
    )  # Category ID of scraped category for category scrape type
    rank = Column(Integer, nullable=False)  # Rank of the video at the time of scrape
    # Foreign key to channels table
    channel_id = Column(String(255), ForeignKey("channels.channel_id"))
    # Foreign key to categories table
    category_id = Column(Integer, ForeignKey("categories.category_id"))

    category = relationship("Categories", back_populates="video_data")
    channel = relationship("Channels", back_populates="video_data")

    __table_args__ = (
        UniqueConstraint(
            "video_id", "scrape_type", "scrape_category", name="uq_video_scrape"
        ),
    )


# Table for historical video data
class VideoHistory(Base):
    __tablename__ = "video_history"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique ID for the row
    video_id = Column(String(255), nullable=False)  # ID of the video
    scraped_at = Column(DateTime, nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=False)
    view_count = Column(BigInteger, nullable=True)
    view_count_delta = Column(BigInteger, default=0)
    like_count = Column(BigInteger, nullable=True)
    like_count_delta = Column(BigInteger, default=0)
    comment_count = Column(Integer, nullable=True)
    comment_count_delta = Column(Integer, default=0)
    duration = Column(Interval, nullable=True)
    tags = Column(ARRAY(String), nullable=True)
    rank = Column(Integer, nullable=False)  # Rank of the video at the time of scrape
    scrape_type = Column(Enum(VideoType), nullable=False)
    scrape_category = Column(Integer, nullable=True)
    channel_id = Column(String(255), nullable=False)
    category_id = Column(Integer, nullable=True)  # Category ID of the video

    days_since_scrape = Column(Float, nullable=True)

    view_growth_per_day = Column(Float, nullable=True)
    like_growth_per_day = Column(Float, nullable=True)
    comment_growth_per_day = Column(Float, nullable=True)


class Categories(Base):
    __tablename__ = "categories"

    category_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False)
    assignable = Column(Boolean, nullable=False)

    video_data = relationship("VideoData", back_populates="category")


class Channels(Base):
    __tablename__ = "channels"

    channel_id = Column(String(255), primary_key=True, index=True)
    scraped_at = Column(
        DateTime, nullable=False, server_default=func.now()
    )  # Timestamp of scrape
    title = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=False)
    view_count = Column(BigInteger, nullable=True)  # Total view count of all videos
    popular_view_count = Column(
        BigInteger, nullable=True
    )  # Total view count of popular videos
    average_views = Column(BigInteger, nullable=True)  # Average views per popular video
    subscriber_count = Column(BigInteger, nullable=True)
    video_count = Column(Integer, nullable=True)
    popular_count = Column(
        Integer, nullable=True
    )  # Number of popular videos from channel
    like_count = Column(BigInteger, nullable=True)
    comment_count = Column(BigInteger, nullable=True)
    average_likes = Column(BigInteger, nullable=True)
    average_comments = Column(BigInteger, nullable=True)

    video_data = relationship("VideoData", back_populates="channel")


class ChannelHistory(Base):
    __tablename__ = "channel_history"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique ID for the row
    channel_id = Column(String(255), nullable=False)  # ID of the channel
    scraped_at = Column(DateTime, nullable=False)
    title = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=False)
    view_count = Column(BigInteger, nullable=True)
    view_count_delta = Column(BigInteger, nullable=True)
    popular_view_count = Column(BigInteger, nullable=True)
    popular_view_count_delta = Column(BigInteger, nullable=True)
    average_views = Column(BigInteger, nullable=True)
    average_views_delta = Column(BigInteger, nullable=True)
    like_count = Column(BigInteger, nullable=True)
    like_count_delta = Column(BigInteger, nullable=True)
    average_likes = Column(BigInteger, nullable=True)
    average_likes_delta = Column(BigInteger, nullable=True)
    comment_count = Column(BigInteger, nullable=True)
    comment_count_delta = Column(BigInteger, nullable=True)
    average_comments = Column(BigInteger, nullable=True)
    average_comments_delta = Column(BigInteger, nullable=True)
    subscriber_count = Column(BigInteger, nullable=True)
    subscriber_count_delta = Column(BigInteger, nullable=True)
    video_count = Column(Integer, nullable=True)
    video_count_delta = Column(Integer, nullable=True)
    popular_count = Column(Integer, nullable=True)
    popular_count_delta = Column(Integer, nullable=True)

    days_since_scrape = Column(Float, nullable=True)

    view_growth_per_day = Column(Float, nullable=True)
    popular_view_growth_per_day = Column(Float, nullable=True)
    average_view_growth_per_day = Column(Float, nullable=True)
    like_growth_per_day = Column(Float, nullable=True)
    average_like_growth_per_day = Column(Float, nullable=True)
    comment_growth_per_day = Column(Float, nullable=True)
    average_comment_growth_per_day = Column(Float, nullable=True)
    subscriber_growth_per_day = Column(Float, nullable=True)
    video_growth_per_day = Column(Float, nullable=True)
    popular_count_growth_per_day = Column(Float, nullable=True)
