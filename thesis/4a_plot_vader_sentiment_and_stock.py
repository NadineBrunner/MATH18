"""
Plot vader sentiment and stock price per hour.

Usage example:
thesis/4a_plot_vader_sentiment_and_stock.py facebook
"""

from collections import defaultdict
from plotly import graph_objs as go
from plotly.offline import plot
from tqdm import tqdm
import couchdb
import numpy as np
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


# Load the tweets with the sentiments from the database and group them per
# timespan:
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


# Build sorted axis for plotting.
plot_data = {
    'time': [],
    'tweets': [],
    'sentiment': [],
    'stock': []}
for time, sentiment in tqdm(sorted(tweet_sentiments_per_timespan.items()),
                            'Prepare data...'):
    plot_data['time'].append(time)
    plot_data['tweets'].append(len(sentiment))
    plot_data['sentiment'].append(np.mean(sentiment))
    previous_stock = plot_data['stock'] and plot_data['stock'][-1] or None
    plot_data['stock'].append(stock_per_timespan.get(time, previous_stock))


color1 = '#96C3DC'
color2 = '#A4DB78'
color3 = '#F78587'


# Render and display the plot.
plot(
    go.Figure(
        data=[
            go.Scatter(
                x=plot_data['time'],
                y=plot_data['tweets'],
                marker={'color': color1},
                name='Amount of tweets'),
            go.Scatter(
                x=plot_data['time'],
                y=plot_data['sentiment'],
                yaxis='y2',
                marker={'color': color2},
                name='Sentiment of tweets'),
            go.Scatter(
                x=plot_data['time'],
                y=plot_data['stock'],
                yaxis='y3',
                marker={'color': color3},
                name='Stock price'),
        ],

        layout=go.Layout(
            title=BRAND,
            xaxis={'title': 'Time',
                   'domain': [0, 0.9]},
            yaxis1={'title': 'Amount of tweets',
                    'showgrid': False,
                    'titlefont': dict(color=color1),
                    'tickfont': dict(color=color1)},
            yaxis2={'title': 'Sentiment of tweets',
                    'overlaying': 'y',
                    'side': 'right',
                    'titlefont': dict(color=color2),
                    'tickfont': dict(color=color2)},
            yaxis3={'title': 'Stock price',
                    'showgrid': False,
                    'overlaying': 'y',
                    'side': 'right',
                    'position': 0.95,
                    'titlefont': dict(color=color3),
                    'tickfont': dict(color=color3)},
        )),
    filename='plot/{}_sentiment.html'.format(BRAND))
