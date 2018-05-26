"""
Create a new brand database.

The goal of this script is to create all necessary database for one brand in
CouchDB.
Only one-word brands are supported.

Usage example:
thesis/1_create_brand_databases.py facebook
"""

from textwrap import dedent
import couchdb
import sys


brand = sys.argv[1]
couchdb_connection = couchdb.Server()

twitter_db_name = 'mt-twitter-' + brand
if twitter_db_name in couchdb_connection:
    print('Database {} already exists.'.format(twitter_db_name))
else:
    print('Creating database {}.'.format(twitter_db_name))
    # Create the database in CouchDB
    twitter_database = couchdb_connection.create(twitter_db_name)
    # Add some views so that we can make fast queries later.
    # The view functions are implemented in JavaScript.
    twitter_database.save({
        '_id': '_design/vader_sentiment',
        'views': {
            'with': {
                'map': dedent('''
                        function(doc) {
                          if (doc.vader_sentiment || doc.vader_sentiment == 0) {
                            emit(doc.created_at, doc.vader_sentiment);
                          }
                        }
                       ''').strip()},
            'without': {
                'map': dedent('''
                        function(doc) {
                          if (!doc.vader_sentiment && doc.vader_sentiment!=0) {
                            emit(doc.created_at, doc._id);
                          }
                        }
                       ''').strip()},
        },
        'language': 'javascript'})


stock_db_name = 'mt-stock-' + brand
if stock_db_name in couchdb_connection:
    print('Database {} already exists.'.format(stock_db_name))
else:
    print('Creating database {}.'.format(stock_db_name))
    # Create the database in CouchDB
    stock_database = couchdb_connection.create(stock_db_name)
