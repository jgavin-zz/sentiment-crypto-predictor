FROM python:2

RUN pip install --upgrade pip
RUN pip install --upgrade google-api-python-client
RUN pip install python-dateutil
RUN pip install requests

ADD btc-price-ingest/btc-price-ingest.py /btc-price-ingest.py
ADD python-utils/utils.py /utils.py

CMD python btc-price-ingest.py