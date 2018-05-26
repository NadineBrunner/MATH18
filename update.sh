#!/usr/bin/env sh
set -xeuo pipefail

bin/python thesis/2b_twitter_to_couchdb_update.py tesla
bin/python thesis/2b_twitter_to_couchdb_update.py facebook
bin/python thesis/2b_twitter_to_couchdb_update.py amazon

bin/python thesis/2c_stock_price_to_couchdb.py tesla TSLA
bin/python thesis/2c_stock_price_to_couchdb.py facebook FB
bin/python thesis/2c_stock_price_to_couchdb.py amazon AMZN

bin/python thesis/3a_twitter_sentiment_analysis_vader.py tesla
bin/python thesis/3a_twitter_sentiment_analysis_vader.py facebook
bin/python thesis/3a_twitter_sentiment_analysis_vader.py amazon

bin/python thesis/4a_plot_vader_sentiment_and_stock.py tesla TSLA
bin/python thesis/4a_plot_vader_sentiment_and_stock.py facebook FB
bin/python thesis/4a_plot_vader_sentiment_and_stock.py amazon AMZN

bin/python thesis/5a_plot_vader_stock_correlation.py tesla
bin/python thesis/5a_plot_vader_stock_correlation.py facebook
bin/python thesis/5a_plot_vader_stock_correlation.py amazon
