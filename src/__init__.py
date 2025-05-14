from dotenv import load_dotenv
import os

# Load environment variables from a .env file
load_dotenv()

# Access the API key from the environment
api_key = os.getenv("YOUTUBE_API_KEY")

# Define the database URL
database_url = "postgresql+psycopg2://postgres:postgres@localhost:5432/youtube_db"
