#!/bin/bash

IMAGE=$1
sudo docker build -t gcr.io/[PROJECT-ID]/$IMAGE -f $IMAGE/Dockerfile .