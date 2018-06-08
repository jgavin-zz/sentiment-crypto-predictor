FROM google/cloud-sdk:latest

RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl

ADD tools/create-tweet-ime-processor-cluster.sh /create-tweet-ime-processor-cluster.sh
ADD tools/run-tweet-ingest.sh /run-tweet-ingest.sh
ADD tools/run-tweet-model.sh /run-tweet-model.sh
ADD tools/run-tweet-enhance.sh /run-tweet-enhance.sh
ADD tweet-ime-startup/startup.sh /startup.sh 
ADD tweet-ingest/tweet-ingest.yaml /tweet-ingest/tweet-ingest.yaml
ADD tweet-model/tweet-model.yaml /tweet-model/tweet-model.yaml
ADD tweet-enhance/tweet-enhance.yaml /tweet-enhance/tweet-enhance.yaml

RUN chmod +x /create-tweet-ime-processor-cluster.sh
RUN chmod +x /run-tweet-ingest.sh
RUN chmod +x /run-tweet-model.sh
RUN chmod +x /run-tweet-enhance.sh
RUN chmod +x /startup.sh

CMD /startup.sh