"""
Calculate sentiment analysis of tweets with Vader SentimentIntensityAnalyzer.

The goal of this script is to determine the sentiment of the tweets and store it
as a floating number between -1.0 (very negative) and 1.0 (very positive).

The sentiment is extracted with Vader from NLTK (natural language toolkit for
Python) using the SentimentIntensityAnalyzer, which is a pre-trainend algorithm
for sentiment analysis.
The sentiment is stored as "vader_sentiment" attribute for each tweet document.

Usage example:
thesis/3a_twitter_sentiment_analysis_vader.py facebook
"""


from nltk.sentiment.vader import SentimentIntensityAnalyzer
from tqdm import tqdm
import couchdb
import sys


BRAND = sys.argv[1]
COUCH_DATABASE_NAME = 'mt-twitter-' + BRAND

# Establish connection to CouchDB and select the database to write into.
# The database must already exist; create it manually in the CouchDB control
# panel first.
database = couchdb.Server()[COUCH_DATABASE_NAME]

# Instantiate a sentiment intensity analyzer.
sentiment_analyzer = SentimentIntensityAnalyzer()

# Process the tweets in batches so that we are not loading all the tweets into
# our RAM at once.
tweets_per_batch = 1000

# First count the amount of tweets but without loading the documents:
num_of_tweets = len(database.view('vader_sentiment/without'))
print('{} tweets to process'.format(num_of_tweets))

# Make a progress bar (tqdm) with the amount of batches:
for i in tqdm(range(int(num_of_tweets / tweets_per_batch) + 1)):
    # load a batch of tweets for processing:
    entries = database.view('vader_sentiment/without', include_docs=True,
                            limit=tweets_per_batch)

    if len(entries) == 0:
        print('Finished.')
        break

    for view_entry in tqdm(entries):
        # Get the tweet from the document,
        tweet = view_entry.doc
        # run the vader sentiment analyzer and store the compound sentiment
        # in the tweet
        tweet['vader_sentiment'] = sentiment_analyzer.polarity_scores(
            tweet['text'])['compound']
        # and save the tweet back to the database.
        database.save(tweet)
