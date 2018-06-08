FROM google/cloud-sdk:latest

ADD tools/delete-tweet-ime-processor-cluster.sh /delete-tweet-ime-processor-cluster.sh
ADD tweet-ime-shutdown/shutdown.sh /shutdown.sh 
RUN chmod +x /shutdown.sh
RUN chmod +x /delete-tweet-ime-processor-cluster.sh

CMD /shutdown.sh