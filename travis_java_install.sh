#!/bin/bash

# borrowed from: https://github.com/mansenfranzen/pywrangler/blob/master/tests/travis_java_install.sh

# Kafka requires Java 8 in order to work properly. However, TravisCI's Ubuntu
# 16.04 ships with Java 11 and Java can't be set with `jdk` when python is
# selected as language. Ubuntu 14.04 does not work due to missing python 3.7
# support on TravisCI which does have Java 8 as default.

# show current JAVA_HOME and java version
echo "Current JAVA_HOME: $JAVA_HOME"
echo "Current java -version:"
java -version

# install Java 8
#sudo add-apt-repository -y ppa:openjdk-r/ppa
#sudo apt-get -qq update
#sudo apt-get install -y openjdk-8-jdk --no-install-recommends

echo "Updating java alternatives..."
sudo update-java-alternatives -l
sudo update-java-alternatives --help
sudo update-java-alternatives -s java-1.8.0-openjdk-amd64

echo "update-alternatives --config java"
sudo update-alternatives --config java

echo "New java -version"
java -version

echo "Updating JAVA_HOME"
# change JAVA_HOME to Java 8
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

echo "New new java -version"
java -version
