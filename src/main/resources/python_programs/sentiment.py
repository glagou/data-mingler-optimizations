import textblob

from textblob import TextBlob

def analyze_sentiment(comment):
    sentiment_score = TextBlob(comment).sentiment.polarity
    return sentiment_score