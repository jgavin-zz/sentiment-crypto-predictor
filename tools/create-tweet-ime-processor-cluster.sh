#!/bin/bash

gcloud container clusters create tweet-ime-processor --num-nodes=4 --scopes compute-rw,monitoring,logging-write,storage-ro,https://www.googleapis.com/auth/ndev.clouddns.readwrite,bigquery,https://www.googleapis.com/auth/pubsub