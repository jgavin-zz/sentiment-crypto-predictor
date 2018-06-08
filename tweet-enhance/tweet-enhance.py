import base64
import datetime
import json
import os
import time
import threading
import utils
from googleapiclient.discovery import build

PROJECT_ID = os.environ['PROJECT_ID']
PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC_MODEL']
API_KEY = os.environ['ML_API_KEY']
NUM_RETRIES = 3

def enhance_tweets(ml_api_service, pubsub, model_sub_name, bigquery):
    """Write the data to BigQuery in small chunks."""
    tweets = []
    CHUNK = 50  # The size of the BigQuery insertion batch.
    # If no data on the subscription, the time to sleep in seconds
    # before checking again.
    WAIT = 2
    tweet = None
    count = 0
    count_max = 50000
    while count < count_max:
        while len(tweets) < CHUNK:
            twmessages = utils.pull_messages_from_pubsub(pubsub, PROJECT_ID, model_sub_name)
            if twmessages:
                for res in twmessages:
                    try:
                        tweet = json.loads(res)
                        enhanced_tweet = enhance_tweet(ml_api_service, tweet)
                        tweets.append(enhanced_tweet)
                    except Exception, bqe:
                        print bqe
            else:
                # pause before checking again
                print 'sleeping...'
                time.sleep(WAIT)
        response = utils.bq_data_insert_rows(bigquery, PROJECT_ID, os.environ['BQ_DATASET'],
                             os.environ['BQ_TABLE'], tweets)
        tweets = []
        count += 1
        if count % 25 == 0:
            print ("processing count: %s of %s at %s: %s" %
                   (count, count_max, datetime.datetime.now(), response))

def enhance_tweet(ml_api_service, tweet):
    response = ml_api_service.documents().analyzeSentiment(
        body={
            'document': {
            'type': 'PLAIN_TEXT',
            'content': tweet['text']
        }
    }).execute()
    tweet['polarity'] = response['documentSentiment']['polarity']
    tweet['magnitude'] = response['documentSentiment']['magnitude']
    return tweet

if __name__ == '__main__':
    model_topic_info = PUBSUB_TOPIC.split('/')
    model_topic_name = model_topic_info[-1]
    model_sub_name = "tweets-%s" % model_topic_name
    print "starting write to BigQuery...."
    credentials = utils.get_credentials()
    bigquery = utils.create_bigquery_client(credentials)
    pubsub = utils.create_pubsub_client(credentials)
    ml_api_service = build('language', 'v1beta1', developerKey=API_KEY)
    try:
        # TODO: check if subscription exists first
        subscription = utils.create_subscription(pubsub, PROJECT_ID, model_sub_name, PUBSUB_TOPIC)
    except Exception, e:
        print e
    enhance_tweets(ml_api_service, pubsub, model_sub_name, bigquery)
    print 'exited write loop'