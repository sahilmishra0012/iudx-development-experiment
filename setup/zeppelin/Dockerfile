FROM apache/zeppelin:0.9.0
USER root
RUN apt-get update && apt-get install sudo \
    && wget https://www.python.org/ftp/python/3.6.12/Python-3.6.12.tgz

RUN apt-get -y install gcc \
    && apt-get -y install libssl-dev \
    && apt-get -y install libbz2-dev \
    && apt-get -y install libssl-dev \
    && apt-get -y install zlib1g-dev \
    && tar -xvf Python-3.6.12.tgz && cd Python-3.6.12 \
    && ./configure && make && make install

RUN pip3.6 install  numpy scipy matplotlib ipython jupyter pandas sympy nose
