#!/bin/bash

gcloud container clusters create tweet-ime-scheduler --num-nodes=1 --machine-type g1-small --service-account tweet-ime-scheduler@[PROJECT-ID].iam.gserviceaccount.com