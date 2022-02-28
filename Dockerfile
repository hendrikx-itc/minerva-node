FROM python:3.6

RUN pip3 install pip==21.3.1 minerva-etl

COPY . /src
RUN pip3 install /src

RUN mkdir /etc/minerva -p
COPY dev-stack/node.yml /etc/minerva/node.yml
VOLUME /data

CMD ["minerva-node"]
