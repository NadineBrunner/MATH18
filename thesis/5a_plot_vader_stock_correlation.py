"""
Plot correlation between vader sentiment and stock price.

Usage example:
thesis/5a_plot_vader_stock_correlation.py facebook
"""

from collections import defaultdict
from datetime import datetime
from plotly import graph_objs as go
from plotly.offline import plot
from scipy import stats
from tqdm import tqdm
import couchdb
import numpy as np
import pandas as pd
import sys
import utils


BRAND = sys.argv[1]
TWITTER_COUCH_DATABASE_NAME = 'mt-twitter-' + BRAND
STOCK_COUCH_DATABASE_NAME = 'mt-stock-' + BRAND


# Establish connection to CouchDB and select the databases to write into.
# The database must already exist; create it manually in the CouchDB control
# panel first.
couchdb_connection = couchdb.Server()
twitter_database = couchdb_connection[TWITTER_COUCH_DATABASE_NAME]
stock_database = couchdb_connection[STOCK_COUCH_DATABASE_NAME]


# Load the tweets with the sentiments from the database and group them
# per timespan:
tweet_sentiments_per_timespan = defaultdict(list)
tweets = twitter_database.view('vader_sentiment/with')
for item in tqdm(tweets, 'Loading tweets..'):
    tweet_sentiments_per_timespan[utils.hour_from_string(item.key)].append(
        item.value)


# Load the stock data.
mango_query = {'selector': {'_id': {'$gt': None}},
               'limit': 10**10}
stock_per_timespan = defaultdict(list)
for item in tqdm(stock_database.find(mango_query), 'Loading stock data..'):
    stock_per_timespan[utils.hour_from_string(item['time'])] = float(
        item['price'])

# Use timestamp as x-axis by using them as indexes for the panda series.
timestamps = list(sorted(tweet_sentiments_per_timespan))
# Focus on the time range where we have both, stock and tweets, while still
# supporting gaps in stocks.
timestamps_with_tweets_and_stock = sorted(set(tweet_sentiments_per_timespan)
                                          & set(stock_per_timespan))
timestamps = timestamps[timestamps.index(timestamps_with_tweets_and_stock[0]):
                        timestamps.index(timestamps_with_tweets_and_stock[-1])]

# The data is incomplete between 2018-04-17 and 2018-04-29 because of a bad
# twitter search query. We need to filter those days.
filter_start_day = datetime(2018, 4, 17)
filter_end_day = datetime(2018, 4, 29, 23, 59)
timestamps = [stamp for stamp in timestamps
              if stamp < filter_start_day or stamp > filter_end_day]

# Prepare series with "Not a Number" values so that the panda series can close
# gaps.
sentiment_series = pd.Series([np.nan] * len(timestamps), index=timestamps)
stock_series = pd.Series([np.nan] * len(timestamps), index=timestamps)

# Fill series with data.
for key in tqdm(timestamps, 'Prepare data...'):
    sentiment_series[key] = np.mean(tweet_sentiments_per_timespan[key])
    if key in stock_per_timespan:
        stock_series[key] = stock_per_timespan[key]


# Interpolate the stock series as it is not complete.
stock_series = stock_series.interpolate(method='time')

# Calculate and print the Spearman's rank correlation coefficient
spearman_r, spearman_p = stats.spearmanr(sentiment_series.values,
                                         stock_series.values)
with open('plot/{}_spearman.txt'.format(BRAND), 'w+') as fio:
    fio.write('Spearman:\nr = {},\np = {}\n'.format(spearman_r, spearman_p))

# Calculate and print the Spearman's rank correlation coefficient
# (maxlag is in hours since input data is in hours)
with open('plot/{}_granger.txt'.format(BRAND), 'w+') as fio:
    fio.write('Sentiment => Stock\n\n')
    utils.granger(sentiment_series.values, stock_series.values, fio, maxlag=96)
with open('plot/{}_granger_reverse.txt'.format(BRAND), 'w+') as fio:
    fio.write('Stock => Sentiment\n\n')
    utils.granger(stock_series.values, sentiment_series.values, fio, maxlag=96)

# Calculate and plot the linear regression
slope, intercept, r_value, p_value, std_err = stats.linregress(sentiment_series,
                                                               stock_series)
line = slope * sentiment_series + intercept

color1 = '#96C3DC'
color2 = '#A4DB78'

plot(go.Figure(
    data=[
        go.Scatter(
            x=sentiment_series,
            y=stock_series,
            marker={'color': color1},
            mode='markers',
            name='Sentiment vs. Price'),
        go.Scatter(
            x=sentiment_series,
            y=line,
            mode='lines',
            marker={'color': color2},
            name='Fit')],
    layout=go.Layout(
        title=('{}: Correlation of tweet sentiment and'
               ' stock closing price.').format(BRAND),
    )),
     filename='plot/{}_linear_regression.html'.format(BRAND))
