#!/usr/bin/env python3
"""
Tag analysis script for YouTube video data
"""

from collections import Counter
from sqlalchemy import create_engine
import pandas as pd
from src.database.database import SessionLocal
from src.database.models import VideoData

def analyze_tags():
    """Analyze tag popularity and trends"""
    session = SessionLocal()
    
    try:
        # Get all videos with tags
        videos = session.query(VideoData.tags, VideoData.view_count, VideoData.like_count).filter(
            VideoData.tags.isnot(None)
        ).all()
        
        # Extract all tags
        all_tags = []
        tag_metrics = []
        
        for video in videos:
            if video.tags:
                video_tags = [tag.strip() for tag in video.tags.split(',')]
                all_tags.extend(video_tags)
                
                # Store metrics for each tag
                for tag in video_tags:
                    tag_metrics.append({
                        'tag': tag.strip(),
                        'view_count': video.view_count or 0,
                        'like_count': video.like_count or 0
                    })
        
        # Most popular tags by frequency
        tag_counts = Counter(all_tags)
        print("TOP 20 MOST FREQUENT TAGS:")
        print("-" * 40)
        for tag, count in tag_counts.most_common(20):
            print(f"{tag}: {count} videos")
        
        # Most popular tags by total views
        df = pd.DataFrame(tag_metrics)
        tag_views = df.groupby('tag').agg({
            'view_count': ['sum', 'mean', 'count'],
            'like_count': ['sum', 'mean']
        }).round(0)
        
        tag_views.columns = ['total_views', 'avg_views', 'video_count', 'total_likes', 'avg_likes']
        tag_views = tag_views.sort_values('total_views', ascending=False)
        
        print("\n\nTOP 20 TAGS BY TOTAL VIEWS:")
        print("-" * 40)
        print(tag_views.head(20))
        
        return tag_counts, tag_views
        
    finally:
        session.close()

def search_videos_by_tags(search_tags, operator='AND'):
    """
    Search videos by tags
    
    Args:
        search_tags: list of tags to search for
        operator: 'AND' or 'OR' - how to combine tag searches
    """
    session = SessionLocal()
    
    try:
        query = session.query(VideoData)
        
        if operator == 'AND':
            # Video must have ALL tags
            for tag in search_tags:
                query = query.filter(VideoData.tags.like(f'%{tag}%'))
        else:  # OR
            # Video must have ANY of the tags
            conditions = [VideoData.tags.like(f'%{tag}%') for tag in search_tags]
            from sqlalchemy import or_
            query = query.filter(or_(*conditions))
        
        results = query.all()
        
        print(f"\nFound {len(results)} videos with tags: {search_tags} ({operator})")
        for video in results[:10]:  # Show first 10
            print(f"- {video.title}: {video.tags}")
        
        return results
        
    finally:
        session.close()

def get_tag_statistics():
    """Get overall tag statistics"""
    session = SessionLocal()
    
    try:
        # Basic stats
        total_videos = session.query(VideoData).count()
        videos_with_tags = session.query(VideoData).filter(VideoData.tags.isnot(None)).count()
        videos_without_tags = total_videos - videos_with_tags
        
        print("TAG STATISTICS:")
        print("-" * 30)
        print(f"Total videos: {total_videos}")
        print(f"Videos with tags: {videos_with_tags} ({videos_with_tags/total_videos*100:.1f}%)")
        print(f"Videos without tags: {videos_without_tags} ({videos_without_tags/total_videos*100:.1f}%)")
        
        # Get all unique tags
        videos = session.query(VideoData.tags).filter(VideoData.tags.isnot(None)).all()
        all_tags = set()
        for video in videos:
            if video.tags:
                tags = [tag.strip() for tag in video.tags.split(',')]
                all_tags.update(tags)
        
        print(f"Unique tags: {len(all_tags)}")
        
    finally:
        session.close()

if __name__ == "__main__":
    # Run analysis
    get_tag_statistics()
    print("\n" + "="*50 + "\n")
    
    tag_counts, tag_views = analyze_tags()
    
    print("\n" + "="*50 + "\n")
    
    # Example searches
    search_videos_by_tags(['music', 'pop'], operator='AND')
    search_videos_by_tags(['gaming', 'tutorial'], operator='OR')
