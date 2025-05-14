# data preprocessing file
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# preprocessing functions
def clean_text(text):
    # Remove special characters and digits
    text = ''.join(e for e in text if e.isalpha() or e.isspace())
    # Convert to lowercase
    return text.lower()

def analyze_sentiment(text):
    # Initialize the sentiment intensity analyzer
    sia = SentimentIntensityAnalyzer()
    # Get the sentiment scores
    sentiment_scores = sia.polarity_scores(text)
    # Return the compound score
    return sentiment_scores['compound']

