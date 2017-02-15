FROM ubuntu:xenial
RUN /usr/sbin/locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu precise main" >> /etc/apt/sources.list
RUN echo "deb http://ppa.launchpad.net/fkrull/deadsnakes/ubuntu precise main" >> /etc/apt/sources.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 5BB92C09DB82666C C2518248EEA14886
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
RUN apt-get update && apt-get install -y python2.7-dev \
    python3.5-dev \
    pypy-dev \
    python-pkg-resources \
    python-setuptools \
    python-virtualenv \
    libsnappy-dev \
    oracle-java8-installer \
    wget\
    g++ \
    ca-certificates \
    python-pip \
    python-tox

ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle"
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
RUN chmod +x /work/run_itest.sh
RUN chmod +x /work/run_utest.sh

WORKDIR /work
