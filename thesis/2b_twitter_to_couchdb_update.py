"""
Update CouchDB with newest tweets.

The goal of this script is to update new tweets into CouchDB with existing
tweets.
The results of Twitter's search API endpoint are ordered from newest to oldest.
We usually start the import where the newest tweet is somewhere in the past and
we start from now working backwards until we reach the tweet which was
previously the newest, so that the gap is closed.

The challenge is to build the script robust enough, so that it can crash (e.g.
because of network problems, unhandled exceptions, rate limits, etc.) and can
recover when it is restarted.

In order to provide this robustness we start an update session by remembering
the previously newest tweet and import backwards in time until we either reach
this tweet or an older one. We also store the oldest tweet we have imported in
our session, so that we can easily recover and restart from the last successful
position.

In order to detect whether a tweet is older or newer than another tweet, we
rely on that fact that the tweet ID's are strictly monotonous growing.

The script is very similar to 01_twitter_to_couchdb_initial.py.
Main differences:
- session handling
- max_id query parameter
- abort condition

Usage example:
thesis/2b_twitter_to_couchdb_update.py facebook

The brand (facebook) can be replaced with any brand name.
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

# The SESSION_STATE_FILE contains the path to a file where infos about the
# import session are stored.
# If the SESSION_STATE_FILE exists, we are currently in the middle of an import
# session and the file describes the gap between the previously already
# existing tweets and the newer tweets we are currently importing.
# The file contains a JSON string with these infos:
# - "previously_newest_tweet": the ID of the tweet which was the newest before
#   we started
#   the current import session
# - "session_oldest_tweet": the ID of the oldest tweet imported in this session,
#   which is
#   normally the last imported tweet.
SESSION_STATE_FILE = Path(__file__).joinpath('..', '..', 'session_state_' +
                                             BRAND + '.json').abspath()

if SESSION_STATE_FILE.exists():
    # There is already an active session; load the session state from the file
    # and continue where we stopped.
    SESSION_STATE = json.loads(SESSION_STATE_FILE.bytes())
else:
    # We are stating a new import session, so lets start by writing an session
    # state file with the currently newest tweet ID.
    document_ids = tuple(filter(lambda id_: not id_.startswith('_'), database))
    SESSION_STATE = {'previously_newest_tweet': max(document_ids),
                     'session_oldest_tweet': None}
    SESSION_STATE_FILE.write_text(json.dumps(SESSION_STATE))


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

    # Use the session_oldest_tweet as max_id in the twitter query.
    if SESSION_STATE['session_oldest_tweet']:
        twitter_query.set_max_id(int(SESSION_STATE['session_oldest_tweet']))
        print('Updating tweets older than {}'.format(
            SESSION_STATE['session_oldest_tweet']))
    else:
        print('Start new update session.')

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

    if twitter_result_stream.get_amount_of_tweets() == 0:
        # There are no new tweets with this query, so we can terminate the
        # import.
        print('Import finished, terminating session.')
        # We are removing the session file in order to terminate the session,
        # so that the next run begins a fresh session.
        SESSION_STATE_FILE.remove()
        # We exit the program with an exit code of 0, indicating that everything
        # was successful.
        sys.exit(0)

    # Track some statistics for displaying the progress:
    num_processed = 0
    num_imported = 0

    # Now we import the tweets into our CouchDB.
    # We use tqdm for displaying status information about how many objects were
    # processed.
    for tweet in tqdm(twitter_result_stream):
        num_processed += 1

        # If we have reached previously_newest_tweet or an older tweet, we can
        # abort the import because we already have imported all tweets from
        # there.
        # The IDs are actually numbers and should be compared as numbers (int),
        # not as text.
        if int(tweet['id']) < int(SESSION_STATE['previously_newest_tweet']):
            print('Import finished, terminating session.')
            # We are removing the session file in order to terminate the
            # session, so that the next run begins a fresh session.
            SESSION_STATE_FILE.remove()
            # We exit the program with an exit code of 0, indicating that
            # everything was successful.
            sys.exit(0)

        # Use the twitter "id" as CouchDB document "_id" (primary key) in order
        # to avoid duplicates.
        tweet['_id'] = str(tweet['id'])
        if tweet['_id'] not in database:
            # The tweet does not yet exist in our database, therefore we are
            # saving it.
            num_imported += 1
            database.save(tweet)
            # We update the session data and store it to the session file so
            # that we can continue from this point when we are recovering the
            # session (restarting the import).
            SESSION_STATE['session_oldest_tweet'] = tweet['_id']
            SESSION_STATE_FILE.write_text(json.dumps(SESSION_STATE))

    print('Imported {} of {} tweets.'.format(num_imported, num_processed))
