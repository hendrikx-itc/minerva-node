FROM python:3.6
MAINTAINER HENDRIKX-ITC

RUN pip3 install minerva-node

RUN mkdir /etc/minerva -p
COPY node.yml /etc/minerva/node.yml
VOLUME /data

CMD ["minerva-node"]
