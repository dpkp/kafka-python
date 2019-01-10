#!/bin/bash -e

export KAFKA_VERSION='1.1.0'
./build_integration.sh
tox -e py27
tox -e py35
tox -e pypy
