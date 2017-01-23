#!/bin/bash

# Versions available for testing via binary distributions
OFFICIAL_RELEASES="0.8.1.1 0.8.2.2 0.9.0.1 0.10.1.1"

# Useful configuration vars, with sensible defaults
if [ -z "$SCALA_VERSION" ]; then
  SCALA_VERSION=2.10
fi

# On travis CI, empty KAFKA_VERSION means skip integration tests
# so we dont try to get binaries
# Otherwise it means test all official releases, so we get all of them!
if [ -z "$KAFKA_VERSION" -a -z "$TRAVIS" ]; then
  KAFKA_VERSION=$OFFICIAL_RELEASES
fi

# By default look for binary releases at archive.apache.org
if [ -z "$DIST_BASE_URL" ]; then
  DIST_BASE_URL="https://archive.apache.org/dist/kafka/"
fi

# When testing against source builds, use this git repo
if [ -z "$KAFKA_SRC_GIT" ]; then
  KAFKA_SRC_GIT="https://github.com/apache/kafka.git"
fi

pushd servers
  mkdir -p dist
  pushd dist
    for kafka in $KAFKA_VERSION; do
      if [ "$kafka" == "trunk" ]; then
        if [ ! -d "$kafka" ]; then
          git clone $KAFKA_SRC_GIT $kafka
        fi
        pushd $kafka
          git pull
          ./gradlew -PscalaVersion=$SCALA_VERSION -Pversion=$kafka releaseTarGz -x signArchives
        popd
        # Not sure how to construct the .tgz name accurately, so use a wildcard (ugh)
        tar xzvf $kafka/core/build/distributions/kafka_*.tgz -C ../$kafka/
        rm $kafka/core/build/distributions/kafka_*.tgz
        rm -rf ../$kafka/kafka-bin
        mv ../$kafka/kafka_* ../$kafka/kafka-bin
      else
        echo "-------------------------------------"
        echo "Checking kafka binaries for ${kafka}"
        echo
        # kafka 0.8.0 is only available w/ scala 2.8.0
        if [ "$kafka" == "0.8.0" ]; then
          KAFKA_ARTIFACT="kafka_2.8.0-${kafka}"
        else
          KAFKA_ARTIFACT="kafka_${SCALA_VERSION}-${kafka}"
        fi
        if [ ! -f "../$kafka/kafka-bin/bin/kafka-run-class.sh" ]; then
          echo "Downloading kafka ${kafka} tarball"
          if hash wget 2>/dev/null; then
            wget --no-proxy --tries=3 -N https://archive.apache.org/dist/kafka/$kafka/${KAFKA_ARTIFACT}.tgz || wget --no-proxy --tries=3 -N https://archive.apache.org/dist/kafka/$kafka/${KAFKA_ARTIFACT}.tar.gz
          else
            echo "wget not found... using curl"
            if [ -f "${KAFKA_ARTIFACT}.tar.gz" ]; then
              echo "Using cached artifact: ${KAFKA_ARTIFACT}.tar.gz"
            else
              curl -f https://archive.apache.org/dist/kafka/$kafka/${KAFKA_ARTIFACT}.tgz -o ${KAFKA_ARTIFACT}.tar.gz || curl -f https://archive.apache.org/dist/kafka/$kafka/${KAFKA_ARTIFACT}.tar.gz -o ${KAFKA_ARTIFACT}.tar.gz
            fi
          fi
          echo
          echo "Extracting kafka ${kafka} binaries"
          tar xzvf ${KAFKA_ARTIFACT}.t* -C ../$kafka/
          rm -rf ../$kafka/kafka-bin
          mv ../$kafka/${KAFKA_ARTIFACT} ../$kafka/kafka-bin
          if [ ! -f "../$kafka/kafka-bin/bin/kafka-run-class.sh" ]; then
            echo "Extraction Failed ($kafka/kafka-bin/bin/kafka-run-class.sh does not exist)!"
            exit 1
          fi
        else
          echo "$kafka is already installed in servers/$kafka/ -- skipping"
        fi
      fi
      echo
    done
  popd
popd
