#!/bin/bash -e


export KAFKA_VERSION='0.9.0.1'
./build_integration.sh
tox -e py27
tox -e py35
tox -e pypy

export KAFKA_VERSION='0.10.0.0'
./build_integration.sh
tox -e py27
tox -e py35
tox -e pypy
