FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y python3 python3-pip

WORKDIR /main

COPY . /main

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

CMD ["python3", "main.py"]
