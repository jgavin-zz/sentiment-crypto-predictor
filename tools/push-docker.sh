#!/bin/bash

IMAGE=$1
sudo gcloud docker -- push gcr.io/[PROJECT-ID]/$IMAGE