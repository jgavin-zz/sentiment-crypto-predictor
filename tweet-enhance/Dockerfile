FROM python:2

RUN pip install --upgrade pip
RUN pip install tweepy
RUN pip install --upgrade google-api-python-client
RUN pip install python-dateutil

ADD tweet-enhance/tweet-enhance.py /tweet-enhance.py
ADD python-utils/utils.py /utils.py

CMD python tweet-enhance.py