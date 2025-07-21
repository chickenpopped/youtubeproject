from fastapi import FastAPI

from src.database.database import SessionLocal
from src.database.models import ChannelHistory, Channels, VideoData, VideoHistory

session = SessionLocal()

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/video_data")
async def getVideoData() -> list[VideoData]:
    return session.query(VideoData).all()
