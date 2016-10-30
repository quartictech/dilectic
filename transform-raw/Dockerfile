#
# Super simple example of a Dockerfile
#
FROM ubuntu:latest
MAINTAINER Alex Sparrow "alex@quartic.io"

RUN apt-get update
RUN apt-get install -y osm2pgsql postgis unzip python3 python3-pip git libffi-dev

RUN apt-get install -y libpq-dev zip

WORKDIR /home
RUN git clone https://github.com/dilshod/xlsx2csv.git
WORKDIR /home/xlsx2csv
RUN python setup.py install

RUN apt-get install -y catdoc

COPY requirements.txt /home
RUN pip3 install -r /home/requirements.txt


VOLUME ["/home/data/raw", "home/data/out"]

ENV INPUT=/home/data/raw
ENV OUTPUT=/home/data/derived

WORKDIR /home/work
