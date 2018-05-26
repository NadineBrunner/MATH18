#!/usr/bin/env sh
set -xeuo pipefail

bin/python thesis/3a_twitter_sentiment_analysis_vader.py tesla
bin/python thesis/3a_twitter_sentiment_analysis_vader.py facebook
bin/python thesis/3a_twitter_sentiment_analysis_vader.py amazon
