from fastapi import FastAPI
from  src.database.database import SessionLocal

session = SessionLocal()

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/video_data")
async def getVideoData():
    return {
        session.query("")
    }