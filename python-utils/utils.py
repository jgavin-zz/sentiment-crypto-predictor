import datetime
import time
import base64

from apiclient import discovery
import httplib2
from oauth2client.client import GoogleCredentials

SCOPES = ['https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/pubsub']
NUM_RETRIES = 3



def get_credentials():
    """Get the Google credentials needed to access our services."""
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
            credentials = credentials.create_scoped(SCOPES)
    return credentials


def create_bigquery_client(credentials):
    """Build the bigquery client."""
    http = httplib2.Http()
    credentials.authorize(http)
    return discovery.build('bigquery', 'v2', http=http)


def create_pubsub_client(credentials):
    """Build the pubsub client."""
    http = httplib2.Http()
    credentials.authorize(http)
    return discovery.build('pubsub', 'v1beta2', http=http)


def bq_data_insert_rows(bigquery, project_id, dataset, table, items):
    """Insert a list of tweets into the given BigQuery table."""
    try:
        rowlist = []
        # Generate the data that will be sent to BigQuery
        for item in items:
            item_row = {"json": item}
            rowlist.append(item_row)
        body = {"rows": rowlist}
        # Try the insertion.
        response = bigquery.tabledata().insertAll(
                projectId=project_id, datasetId=dataset,
                tableId=table, body=body).execute(num_retries=NUM_RETRIES)
        # print "streaming response: %s %s" % (datetime.datetime.now(), response)
        return response
        # TODO: 'invalid field' errors can be detected here.
    except Exception, e1:
        print "Giving up: %s" % e1

def bq_data_insert_row(bigquery, project_id, dataset, table, item):
    """Insert a list of tweets into the given BigQuery table."""
    try:
        item_row = {"json": item}
        rowlist = [item_row]
        body = {"rows": rowlist}
        # Try the insertion.
        response = bigquery.tabledata().insertAll(
                projectId=project_id, datasetId=dataset,
                tableId=table, body=body).execute(num_retries=NUM_RETRIES)
        # print "streaming response: %s %s" % (datetime.datetime.now(), response)
        return response
        # TODO: 'invalid field' errors can be detected here.
    except Exception, e1:
        print "Giving up: %s" % e1

def publish_to_pubsub(client, pubsub_topic, data_lines):
    """Publish to the given pubsub topic."""
    messages = []
    for line in data_lines:
        pub = base64.urlsafe_b64encode(line)
        messages.append({'data': pub})
    body = {'messages': messages}
    resp = client.projects().topics().publish(
            topic=pubsub_topic, body=body).execute(num_retries=NUM_RETRIES)
    return resp

def fqrn(resource_type, project, resource):
    """Returns a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)

def create_subscription(client, project_name, sub_name, pubsub_name):
    """Creates a new subscription to a given topic."""
    print "using pubsub topic: %s" % pubsub_name
    name = get_full_subscription_name(project_name, sub_name)
    body = {'topic': pubsub_name}
    subscription = client.projects().subscriptions().create(
            name=name, body=body).execute(num_retries=NUM_RETRIES)
    print 'Subscription {} was created.'.format(subscription['name'])

def get_full_subscription_name(project, subscription):
    """Returns a fully qualified subscription name."""
    return fqrn('subscriptions', project, subscription)

def pull_messages_from_pubsub(client, project_name, sub_name):
    """Pulls messages from a given subscription."""
    BATCH_SIZE = 50
    tweets = []
    subscription = get_full_subscription_name(project_name, sub_name)
    body = {
            'returnImmediately': False,
            'maxMessages': BATCH_SIZE
    }
    try:
        resp = client.projects().subscriptions().pull(
                subscription=subscription, body=body).execute(
                        num_retries=NUM_RETRIES)
    except Exception as e:
        print "Exception: %s" % e
        time.sleep(0.5)
        return
    receivedMessages = resp.get('receivedMessages')
    if receivedMessages is not None:
        ack_ids = []
        for receivedMessage in receivedMessages:
                message = receivedMessage.get('message')
                if message:
                        tweets.append(
                            base64.urlsafe_b64decode(str(message.get('data'))))
                        ack_ids.append(receivedMessage.get('ackId'))
        ack_body = {'ackIds': ack_ids}
        client.projects().subscriptions().acknowledge(
                subscription=subscription, body=ack_body).execute(
                        num_retries=NUM_RETRIES)
    return tweets