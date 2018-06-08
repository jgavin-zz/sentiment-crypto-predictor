FROM python:2

RUN pip install --upgrade pip
RUN pip install tweepy
RUN pip install --upgrade google-api-python-client
RUN pip install python-dateutil

ADD tweet-model/tweet-model.py /tweet-model.py
ADD python-utils/utils.py /utils.py

CMD python tweet-model.py