# This file defines the SQLAlchemy models for the YouTube video data and categories.

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Integer,
    Interval,
    String,
    Text,
    DateTime,
    Enum,
    func,
)
from sqlalchemy.orm import relationship

from src.database import Base

import enum

class VideoType(enum.Enum):
    popular = "popular"
    category = "category"

# Table for videos 
class VideoData(Base):
    __tablename__ = "video_data"

    id = Column(String(255), primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    scraped_at = Column(DateTime, nullable=False, server_default=func.now())  # Timestamp scrape
    description = Column(Text, nullable=True)
    published_at = Column(String(50), nullable=False)  # Store as string for simplicity
    view_count = Column(BigInteger, nullable=True)
    like_count = Column(BigInteger, nullable=True)
    comment_count = Column(Integer, nullable=True)
    duration = Column(Interval, nullable=True)  # Stored as ISO8601 duration format
    tags = Column(Text, nullable=True)  # Store as comma-separated string for simplicity
    scrape_type = Column(Enum(VideoType), primary_key=True, nullable=False)  # Column for scrape type, e.g., "popular", or "category"
    rank = Column(Integer, nullable=False)  # Rank of the video at the time of scrape
    # Foreign key to channels table
    channel_id = Column(String(255), ForeignKey("channels.channel_id"))
    # Foreign key to categories table
    category_id = Column(Integer, ForeignKey("categories.category_id"), primary_key=True)

    # Relationship to categories table
    category = relationship("Categories", back_populates="video_data")
    # Relationship to channels table
    channel = relationship("Channels", back_populates="video_data")

# Table for historical video data
class VideoHistory(Base):
    __tablename__ = "video_history"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique ID for the video
    video_id = Column(String(255), nullable=False)  # ID of the video
    scraped_at = Column(DateTime, nullable=False, server_default = func.now())  # Timestamp of scrape
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    published_at = Column(String(50), nullable=False)  # Store as string for simplicity
    view_count = Column(BigInteger, nullable=True)
    view_delta = Column(BigInteger, default=0)  # Change in view count since last scrape
    like_count = Column(BigInteger, nullable=True)
    like_delta = Column(BigInteger, default=0)  # Change in like count since last scrape
    comment_count = Column(Integer, nullable=True)
    comment_delta = Column(Integer, default=0)  # Change in comment count since last scrape
    duration = Column(Interval, nullable=True)  # Stored as ISO8601 duration format
    tags = Column(Text, nullable=True)  # Store as comma-separated string for simplicity
    rank = Column(Integer, nullable=False)  # Rank of the video at the time of scrape
    scrape_type = Column(Enum(VideoType), nullable=False)  # Type of video, e.g., "popular", or "category" 
    
    # Foreign key to channels table
    channel_id = Column(String(255), ForeignKey("channels.channel_id"))
    # Foreign key to categories table
    category_id = Column(Integer, ForeignKey("categories.category_id"))

    # Relationship to channels table
    channel = relationship("Channels", back_populates="video_history")
    # Relationship to categories table
    category = relationship("Categories", back_populates="video_history")


class Categories(Base):
    __tablename__ = "categories"

    category_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False)
    assignable = Column(Boolean, nullable=False)

    # Relationship to VideoData table
    video_data = relationship("VideoData", back_populates="category")
    # Relationship to VideoHistory table
    video_history = relationship("VideoHistory", back_populates="category")


class Channels(Base):
    __tablename__ = "channels"

    channel_id = Column(String(255), primary_key=True, index=True)
    title = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=False, server_default=func.now())  # Timestamp of channel creation
    view_count = Column(BigInteger, nullable=True)
    subscriber_count = Column(BigInteger, nullable=True)
    video_count = Column(Integer, nullable=True)

    # Relationship to VideoData table
    video_data = relationship("VideoData", back_populates="channel")
    # Relationship to VideoHistory table
    video_history = relationship("VideoHistory", back_populates="channel")
