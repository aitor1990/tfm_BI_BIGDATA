FROM java:8-jdk

MAINTAINER AitorMarco

# install dev tools
RUN apt-get install -y wget tar

# get drill
RUN wget http://apache.mirrors.hoobly.com/drill/drill-1.14.0/apache-drill-1.14.0.tar.gz
# create Drill folder
RUN mkdir -p /opt

# extract Drill
RUN tar -xvzf apache-drill-1.14.0.tar.gz -C /opt

ADD entrypoint.sh .

RUN chmod +x entrypoint.sh

EXPOSE 8047
ENTRYPOINT ["/entrypoint.sh"]

