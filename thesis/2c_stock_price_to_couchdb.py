"""
Import stock data for a brand into CouchDB.

The goal of this script is to download stock data from alphavantage and store it
in a separate database per brand.

Usage example:
thesis/2c_stock_price_to_couchdb.py facebook FB
"""

from dateutil.tz import gettz
from path import Path
from tqdm import tqdm
import couchdb
import dateutil.parser
import requests
import sys


BRAND = sys.argv[1]
SYMBOL = sys.argv[2]
COUCH_DATABASE_NAME = 'mt-stock-' + BRAND
ALPHAVANTAGE_KEY = Path(__file__).joinpath(
    '..', '..', 'alphavantage.cfg').abspath().bytes().strip().decode('utf-8')

# Establish connection to CouchDB and select the database to write into.
# The database must already exist; create it manually in the CouchDB control
# panel first.
database = couchdb.Server()[COUCH_DATABASE_NAME]

# Make a HTTP request to alphavantage.co for getting the closing stock price
# per hour:
url = ('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='
       + SYMBOL + '&interval=60min&outputsize=full&apikey=' + ALPHAVANTAGE_KEY)
response = requests.get(url)
response.raise_for_status()
data = response.json()

# Extract the timezohne info so that we can use it for parsing the timestamps.
tzinfo = gettz(data['Meta Data']['6. Time Zone'])

# Iter over each item in the time series:
for time, item in tqdm(data['Time Series (60min)'].items()):
    # Use TTT as temporary mark for our timezone so that we can parse the
    # timestamp correclty:
    time = dateutil.parser.parse(time + ' TTT', tzinfos={'TTT': tzinfo})
    # Build a simple document containing the extract information and the
    # raw data:
    doc = {'time': time.isoformat(),
           'stock': data['Meta Data']['2. Symbol'],
           'price': item['4. close'],
           'volume': item['5. volume'],
           'raw': item}
    # Store it in the database
    database.save(doc)
