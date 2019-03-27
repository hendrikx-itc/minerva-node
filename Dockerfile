FROM python:3.6
MAINTAINER HENDRIKX-ITC

#RUN pip3 install minerva-node
RUN mkdir /minerva-node/
COPY ./ /minerva-node/
RUN cd minerva-node && python setup.py install

VOLUME /data

CMD ["minerva-node"]
