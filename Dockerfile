FROM tutum/debian
MAINTAINER Mark Redar mredar@gmail.com

#TODO: make this a build container. What does that mean for python?
RUN apt-get update
RUN apt-get upgrade -y

RUN apt-get install -y -q git
RUN apt-get install -y -q mercurial
RUN apt-get install -y -q python-dev
RUN apt-get install -y -q python-pip
RUN apt-get install -y -q libxml2-dev
RUN apt-get install -y -q libxslt-dev

RUN mkdir -p /code/dpla/ingestion
WORKDIR /code/dpla
ADD ingestion /code/dpla/ingestion
WORKDIR ingestion
RUN pip install --no-deps --ignore-installed -r requirements.txt

ADD ./akara.ini.tmpl /code/dpla/ingestion/akara.ini

RUN mkdir -p /code/harvester
ADD . /code/harvester
WORKDIR /code/harvester
RUN python setup.py install

ADD ./run.sh /run.sh
RUN chmod 755 /*.sh

EXPOSE 8889 

CMD ["/run.sh"]
