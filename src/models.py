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
)
from sqlalchemy.orm import relationship

from src.database import Base


# Table for videos that are popular in respective category
class CategoryVideos(Base):
    __tablename__ = "category_videos"

    id = Column(String(255), primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    timestamp = Column(String(50), nullable=False)  # Timestamp scrape
    description = Column(Text, nullable=True)
    publishedAt = Column(String(50), nullable=False)  # Store as string for simplicity
    viewCount = Column(BigInteger, nullable=True)
    likeCount = Column(BigInteger, nullable=True)
    commentCount = Column(Integer, nullable=True)
    duration = Column(Interval, nullable=True)  # Stored as ISO8601 duration format
    tags = Column(Text, nullable=True)  # Store as comma-separated string for simplicity

    # Foreign key to channels table
    channelId = Column(String(255), ForeignKey("channels.channelId"))
    # Foreign key to categories table
    categoryId = Column(Integer, ForeignKey("categories.categoryId"))

    # Relationship to popular videos table
    popular_videos = relationship("PopularVideos", back_populates="videos")
    # Relationship to categories table
    category = relationship("Categories", back_populates="category_videos")
    # Relationship to channels table
    channel = relationship("Channels", back_populates="category_videos")


# Table for videos that are popular in general
class PopularVideos(Base):
    __tablename__ = "popular_videos"

    # Foreign key on CategoryVideos table
    id = Column(String(255), ForeignKey("category_videos.id"), primary_key=True)

    # Relationship to CategoryVideos table
    videos = relationship("CategoryVideos", back_populates="popular_videos")


# Table for historical video data
class VideoHistory(Base):
    __tablename__ = "video_history"

    id = Column(
        Integer, primary_key=True, autoincrement=True
    )  # Unique ID for the video
    videoId = Column(String(255), nullable=False)  # ID of the video
    timestamp = Column(String(50), nullable=False)  # Timestamp of scrape
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    publishedAt = Column(String(50), nullable=False)  # Store as string for simplicity
    viewCount = Column(BigInteger, nullable=True)
    likeCount = Column(BigInteger, nullable=True)
    commentCount = Column(Integer, nullable=True)
    duration = Column(Interval, nullable=True)  # Stored as ISO8601 duration format
    tags = Column(Text, nullable=True)  # Store as comma-separated string for simplicity

    # Foreign key to channels table
    channelId = Column(String(255), ForeignKey("channels.channelId"))
    # Foreign key to categories table
    categoryId = Column(Integer, ForeignKey("categories.categoryId"))

    # Relationship to channels table
    channel = relationship("Channels", back_populates="video_history")
    # Relationship to categories table
    category = relationship("Categories", back_populates="video_history")


class Categories(Base):
    __tablename__ = "categories"

    categoryId = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False)
    assignable = Column(Boolean, nullable=False)

    # Relationship to CategoryVideos table
    category_videos = relationship("CategoryVideos", back_populates="category")
    # Relationship to VideoHistory table
    video_history = relationship("VideoHistory", back_populates="category")


class Channels(Base):
    __tablename__ = "channels"

    channelId = Column(String(255), primary_key=True, index=True)
    title = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    publishedAt = Column(String(50), nullable=False)  # Store as string for simplicity
    viewCount = Column(BigInteger, nullable=True)
    subscriberCount = Column(BigInteger, nullable=True)
    videoCount = Column(Integer, nullable=True)

    # Relationship to CategoryVideos table
    category_videos = relationship("CategoryVideos", back_populates="channel")
    # Relationship to VideoHistory table
    video_history = relationship("VideoHistory", back_populates="channel")
