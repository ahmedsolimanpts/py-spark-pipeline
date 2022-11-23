FROM ubuntu

RUN apt update

## Install Python
RUN apt-get install -y python3
RUN apt-get --purge autoremove python3-pip
RUN apt install -y python3-pip

# install java , scala , git and wget
RUN apt install default-jdk scala git wget -y

COPY . /home/myapp

WORKDIR /home/myapp

## Install Requirements
RUN bash ./setup.sh

CMD ["python3","./app.py"]

