#!/bin/bash

CLUSTER_NAME=$1

gcloud container clusters get-credentials $CLUSTER_NAME