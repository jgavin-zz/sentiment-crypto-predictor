import base64
import datetime
import collections
import json
import os
import time
import threading
import utils
import dateutil.parser

# Get the project ID and pubsub topic from the environment variables set in
# the 'bigquery-controller.yaml' manifest.
PROJECT_ID = os.environ['PROJECT_ID']
PUBSUB_TOPIC_INGEST = os.environ['PUBSUB_TOPIC_INGEST']
PUBSUB_TOPIC_MODEL = os.environ['PUBSUB_TOPIC_MODEL']
NUM_RETRIES = 3
FOLLOWER_THRESHOLD = 5000

def flatten(lst):
    """Helper function used to massage the raw tweet data."""
    for el in lst:
        if (isinstance(el, collections.Iterable) and
                not isinstance(el, basestring)):
            for sub in flatten(el):
                yield sub
        else:
            yield el

def cleanup(data):
    """Do some data massaging."""
    if isinstance(data, dict):
        newdict = {}
        for k, v in data.items():
            if (k == 'coordinates') and isinstance(v, list):
                # flatten list
                newdict[k] = list(flatten(v))
            elif k == 'created_at' and v:
                newdict[k] = str(dateutil.parser.parse(v))
            # temporarily, ignore some fields not supported by the
            # current BQ schema.
            # TODO: update BigQuery schema
            elif (k == 'video_info' or k == 'scopes' or k == 'withheld_in_countries'
                  or k == 'is_quote_status' or 'source_user_id' in k
                  or k == ''
                  or 'quoted_status' in k or 'display_text_range' in k or 'extended_tweet' in k
                  or 'media' in k):
                pass
            elif v is False:
                newdict[k] = v
            else:
                if k and v:
                    newdict[k] = cleanup(v)
        return newdict
    elif isinstance(data, list):
        newlist = []
        for item in data:
            newdata = cleanup(item)
            if newdata:
                newlist.append(newdata)
        return newlist
    else:
        return data

def write_to_pubsub(client, tw):
        utils.publish_to_pubsub(client, PUBSUB_TOPIC_MODEL, tw)

def model_tweets(pubsub, ingest_sub_name):
    tweets = []
    CHUNK = 50  # The size of the BigQuery insertion batch.
    # If no data on the subscription, the time to sleep in seconds
    # before checking again.
    WAIT = 2
    tweet = None
    mtweet = None
    count = 0
    count_max = 50000
    while count < count_max:
        while len(tweets) < CHUNK:
            twmessages = utils.pull_messages_from_pubsub(pubsub, PROJECT_ID, ingest_sub_name)
            if twmessages:
                for res in twmessages:
                    try:
                        tweet = json.loads(res)
                    except Exception, bqe:
                        print bqe
                    # First do some massaging of the raw data
                    mtweet = cleanup(tweet)
                    # We only want to write tweets to BigQuery; we'll skip
                    # 'delete' and 'limit' information.
                    if 'delete' in mtweet:
                        continue
                    if 'limit' in mtweet:
                        continue
                    #ignore tweets that don't meet a certain follower threshold
                    if 'user' in mtweet:
                        user = mtweet['user']
                        if 'followers_count' in user:
                            if user['followers_count'] <= FOLLOWER_THRESHOLD:
                                continue
                        else:
                            continue
                    else:
                        continue
                    tweet_string = json.dumps(mtweet)
                    tweets.append(tweet_string)
            else:
                # pause before checking again
                print 'sleeping...'
                time.sleep(WAIT)
        if len(tweets) >= CHUNK:
            write_to_pubsub(pubsub, tweets)
            tweets = []
        count += 1
        if count % 25 == 0:
            print ("processing count: %s of %s at %s" %
                   (count, count_max, datetime.datetime.now()))

if __name__ == '__main__':
    ingest_topic_info = PUBSUB_TOPIC_INGEST.split('/')
    ingest_topic_name = ingest_topic_info[-1]
    ingest_sub_name = "tweets-%s" % ingest_topic_name
    print "starting modeling...."
    credentials = utils.get_credentials()
    pubsub = utils.create_pubsub_client(credentials)
    try:
        # TODO: check if subscription exists first
        subscription = utils.create_subscription(pubsub, PROJECT_ID, ingest_sub_name, PUBSUB_TOPIC_INGEST)
    except Exception, e:
        print e
    model_tweets(pubsub, ingest_sub_name)
    print 'exited write loop'