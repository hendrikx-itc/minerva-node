FROM python:3.6
MAINTAINER HENDRIKX-ITC

#RUN pip3 install minerva-node
# For development we get minerva-node directly from github
RUN pip3 install git+https://github.com/hendrikx-itc/minerva-etl
RUN pip3 install git+https://github.com/hendrikx-itc/minerva-node

RUN mkdir /etc/minerva -p
COPY node.yml /etc/minerva/node.yml
VOLUME /data

CMD ["minerva-node"]
