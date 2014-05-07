#!/bin/bash

git submodule update --init
(cd servers/0.8.0/kafka-src && ./sbt update package assembly-package-dependency)
(cd servers/0.8.1/kafka-src && ./gradlew jar)
