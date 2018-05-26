"""
Initial import from twitter to CouchDB.

The goal of this script is to do the initial import from twitter to our
CouchDDB.
The standard search endpoint of the twitter API usually contians approximately
the last 7 days of history.
This script starts the import with the newest tweet and works its way back to
the oldest tweet available, aprox. 7 days old.
The script may be aborted and restarted; it will continue at the oldest tweet
already imported and imports older tweets from there.

Usage example:
theses/2a_twitter_to_couchdb_initial.py facebook

The brand (facebook) can be replaced with any brand name.
The CouchDB database (mt-twitter-facebook) must be created in advance.
"""

from path import Path
from time import sleep
from tqdm import tqdm
from TwitterSearch import TwitterSearch
from TwitterSearch import TwitterSearchOrder
from TwitterSearch.TwitterSearchException import TwitterSearchException
import couchdb
import json
import sys


BRAND = sys.argv[1]
COUCH_DATABASE_NAME = 'mt-twitter-' + BRAND
TWITTER_SEARCH_KEYWORDS = [BRAND]
TWITTER_CREDENTIALS = json.loads(Path(__file__).joinpath(
    '..', '..', 'twitter.cfg.json').abspath().bytes())


# Establish connection to CouchDB and select the database to write into.
# The database must already exist; create it manually in the CouchDB control
# panel first.
database = couchdb.Server()[COUCH_DATABASE_NAME]

# Setup a twitter connection and configure its credentials:
twitter_connection = TwitterSearch(**TWITTER_CREDENTIALS)

# The twitter client may stop iterating the tweets at some point.
# In order to automatically continue at the last position, we put the
# import in a "while"-loop which will be stopped when there are no new
# tweets to import.
while True:
    # First, let's build a search query:
    twitter_query = TwitterSearchOrder()
    twitter_query.set_keywords(TWITTER_SEARCH_KEYWORDS)
    # Only import english tweets as our sentiment analysis will only work
    # with the English language for now.
    twitter_query.set_language('en')
    # We do not require entities (e.g. extracted URLs) as we are only
    # interested in the raw text of the tweet.
    twitter_query.set_include_entities(False)

    document_ids = tuple(filter(lambda id_: not id_.startswith('_'), database))
    if len(document_ids) > 0:
        # If we already have imported tweets, we should continue with the oldest
        # tweet we know and work our way to older tweets from there.
        # We do that by setting the max_id query parameter to the oldest tweet
        # we know.
        oldest_id = min(document_ids)
        twitter_query.set_max_id(int(oldest_id))
        print('Continuing initial import from tweet {}'.format(oldest_id))
    else:
        print('Starting initial import on fresh database.')


    try:
        # Start making requests to the twitter API by searching tweets with our
        # twitter query.
        twitter_result_stream = twitter_connection.search_tweets_iterable(
            twitter_query)
    except TwitterSearchException as exc:
        if exc.code == 429:
            # Twitter has responded with a "429 Too Many Requests" error.
            # That means we made more requests than twitter allows us to do.
            # See: https://developer.twitter.com/en/docs/basics/rate-limiting
            # We now wait for 100 seconds and then try again until we can make
            # requests again.
            # We use tqdm for displaying the sleep progress.
            for second in tqdm(range(100), 'Sleep because of rate limit'):
                sleep(1)  # sleep for 1 second
            continue
        else:
            # If it is another exception, re-raise the exception so that it is
            # displayed and aborts the import.
            raise

    # Track some statistics for displaying the progress:
    num_processed = 0
    num_imported = 0

    # Now we import the tweets into our CouchDB.
    # We use tqdm for displaying status information about how many objects
    # were processed.
    for tweet in tqdm(twitter_result_stream):
        num_processed += 1

        # Use the twitter "id" as CouchDB document "_id" (primary key) in order
        # to avoid duplicates.
        tweet['_id'] = str(tweet['id'])
        if tweet['_id'] not in database:
            # The tweet does not yet exist in our database, therefore we are
            # saving it.
            num_imported += 1
            database.save(tweet)

    print('Imported {} of {} tweets.'.format(num_imported, num_processed))

    if num_processed == 0:
        print('It seems that we have imported all tweets. Aborting.')
        break
