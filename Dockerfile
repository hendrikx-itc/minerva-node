FROM python:3.6

RUN pip3 install pip==21.3.1 minerva-etl==5.3.2

COPY . /src
WORKDIR /src
RUN pip3 install .

RUN mkdir /etc/minerva -p
COPY dev-stack/node.yml /etc/minerva/node.yml
VOLUME /data

WORKDIR /

CMD ["minerva-node"]
