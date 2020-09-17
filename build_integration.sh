#!/bin/bash

: ${ALL_RELEASES:="0.8.2.2 0.9.0.1 0.10.1.1 0.10.2.2 0.11.0.3 1.0.2 1.1.1 2.0.1 2.1.1 2.2.1 2.3.0 2.4.0 2.5.0"}
: ${SCALA_VERSION:=2.11}
: ${DIST_BASE_URL:=https://archive.apache.org/dist/kafka/}
: ${KAFKA_SRC_GIT:=https://github.com/apache/kafka.git}

# On travis CI, empty KAFKA_VERSION means skip integration tests
# so we don't try to get binaries
# Otherwise it means test all official releases, so we get all of them!
if [ -z "$KAFKA_VERSION" -a -z "$TRAVIS" ]; then
  KAFKA_VERSION=$ALL_RELEASES
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
        if [ "$kafka" == "0.8.0" ]; then
          KAFKA_ARTIFACT="kafka_2.8.0-${kafka}.tar.gz"
        else if [ "$kafka" \> "2.4.0" ]; then
          KAFKA_ARTIFACT="kafka_2.12-${kafka}.tgz"
        else
          KAFKA_ARTIFACT="kafka_${SCALA_VERSION}-${kafka}.tgz"
        fi
        fi
        if [ ! -f "../$kafka/kafka-bin/bin/kafka-run-class.sh" ]; then
          if [ -f "${KAFKA_ARTIFACT}" ]; then
            echo "Using cached artifact: ${KAFKA_ARTIFACT}"
          else
            echo "Downloading kafka ${kafka} tarball"
            TARBALL=${DIST_BASE_URL}${kafka}/${KAFKA_ARTIFACT}
            if command -v wget 2>/dev/null; then
              wget -N $TARBALL
            else
              echo "wget not found... using curl"
              curl -f $TARBALL -o ${KAFKA_ARTIFACT}
            fi
          fi
          echo
          echo "Extracting kafka ${kafka} binaries"
          tar xzvf ${KAFKA_ARTIFACT} -C ../$kafka/
          rm -rf ../$kafka/kafka-bin
          mv ../$kafka/${KAFKA_ARTIFACT/%.t*/} ../$kafka/kafka-bin
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
