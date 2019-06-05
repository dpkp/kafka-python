FROM ubuntu:xenial
ENV DEBIAN_FRONTEND=noninteractive

RUN echo "deb http://ppa.launchpad.net/fkrull/deadsnakes/ubuntu precise main" >> /etc/apt/sources.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 5BB92C09DB82666C
RUN apt-get update && apt-get install -y python2.7-dev \
    python3.5-dev \
    python-pkg-resources \
    python-setuptools \
    python-virtualenv \
    libsnappy-dev \
    locales \
    openjdk-8-jdk \
    wget\
    g++ \
    ca-certificates \
    python-pip \
    python-tox

# python-lz4 requires minium pypy version 5.8.0
RUN wget https://bitbucket.org/pypy/pypy/downloads/pypy2-v5.8.0-linux64.tar.bz2
RUN tar xf pypy2-v5.8.0-linux64.tar.bz2
RUN ln -s $PWD/pypy2-v5.8.0-linux64/bin/pypy /usr/local/bin/pypy

RUN /usr/sbin/locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
ENV PATH="$PATH:$JAVA_HOME/bin"

COPY servers /work/servers
COPY kafka /work/kafka
COPY test /work/test
COPY .covrc /work
COPY pylint.rc /work
COPY README.rst /work
COPY build_integration.sh /work
COPY setup.cfg /work
COPY setup.py /work
COPY tox.ini /work
COPY LICENSE /work
COPY AUTHORS.md /work
COPY CHANGES.md /work
COPY MANIFEST.in /work
COPY run_itest.sh /work
COPY run_utest.sh /work
COPY requirements-dev.txt /work
RUN chmod +x /work/run_itest.sh
RUN chmod +x /work/run_utest.sh

WORKDIR /work
