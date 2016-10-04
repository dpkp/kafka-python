FROM docker-dev.yelpcorp.com/xenial_yelp:latest
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

RUN /usr/sbin/locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle-1.8.0.20"
ENV PATH="$PATH:$JAVA_HOME/bin"
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get install -y python2.7-dev \
    python3.5-dev \
    pypy \
    pypy-dev \
    python-pkg-resources \
    python-pip \
    python-setuptools \
    python-virtualenv \
    python-tox \
    libsnappy-dev \
    java-8u20-oracle \
    wget


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
WORKDIR /work

