#!/bin/bash

TOX_ENV=$1
if [ $1 == "2.7" ]; then
    $TOX_ENV = "py27"
elif [ $1 == "2.6" ]; then
    $TOX_ENV = "py26"
fi;

tox -e $TOX_ENV
KAFKA_VERSION=0.8.0 tox -e $TOX_ENV
KAFKA_VERSION=0.8.1 tox -e $TOX_ENV
if [ $TOX_ENV != "pypy" ]; then
    USE_GEVENT=1 KAFKA_VERSION=0.8.0 tox -e $TOX_ENV
    USE_GEVENT=1 KAFKA_VERSION=0.8.1 tox -e $TOX_ENV
fi;
