from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware
from src.api.dash_app import app as dash_app

from src.database.database import get_session
from src.database.models import ChannelHistory, Channels, VideoData, VideoHistory

from sqlmodel import select


session = get_session()

app = FastAPI()

app.mount("/dash", WSGIMiddleware(dash_app.server))

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/videodata")
async def getVideoData():
    return session.exec(select(VideoData)).all()

temp = type(VideoData)
