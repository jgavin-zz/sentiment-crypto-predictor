#!/bin/bash

gcloud config set project [PROJECT-ID]
gcloud config set compute/zone us-central1-b
./create-tweet-ime-processor-cluster.sh
./run-tweet-ingest.sh
./run-tweet-model.sh
./run-tweet-enhance.sh