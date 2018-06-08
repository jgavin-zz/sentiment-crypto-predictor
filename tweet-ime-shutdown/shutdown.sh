#!/bin/bash

gcloud config set project [PROJECT-ID]
gcloud config set compute/zone us-central1-b
./delete-tweet-ime-processor-cluster.sh