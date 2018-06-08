#!/bin/bash

gcloud container clusters create btc-price-ingest --num-nodes=1 --machine-type g1-small --scopes compute-rw,monitoring,logging-write,storage-ro,https://www.googleapis.com/auth/ndev.clouddns.readwrite,bigquery