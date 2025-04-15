# Some simple testing tasks

SHELL = bash

export KAFKA_VERSION ?= 4.0.0
DIST_BASE_URL ?= https://archive.apache.org/dist/kafka/

# Required to support testing old kafka versions on newer java releases
# The performance opts defaults are set in each kafka brokers bin/kafka_run_class.sh file
# The values here are taken from the 2.4.0 release.
# Note that kafka versions 2.0-2.3 crash on newer java releases; openjdk@11 should work with with "-Djava.security.manager=allow" removed from performance opts
export KAFKA_JVM_PERFORMANCE_OPTS?=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true -Djava.security.manager=allow

PYTESTS ?= 'test'

setup:
	pip install -r requirements-dev.txt
	pip install -Ue .

lint:
	pylint --recursive=y --errors-only kafka test

test: build-integration
	pytest $(PYTESTS)

fixture: build-integration
	python -m test.integration.fixtures kafka

cov-local: build-integration
	pytest --pylint --pylint-rcfile=pylint.rc --pylint-error-types=EF --cov=kafka \
		--cov-config=.covrc --cov-report html $(TEST_FLAGS) kafka test
	@echo "open file://`pwd`/htmlcov/index.html"

# Check the readme for syntax errors, which can lead to invalid formatting on
# PyPi homepage (https://pypi.python.org/pypi/kafka-python)
check-readme:
	python setup.py check -rms

clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf htmlcov
	rm -rf docs/_build/
	rm -rf cover
	rm -rf dist

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: all test test-local cov-local clean doc dist publish

kafka_artifact_version=$(lastword $(subst -, ,$(1)))

# Mappings for artifacts -> scala version; any unlisted will use default 2.12
kafka_scala_0_8_0=2.8.0
kafka_scala_0_8_1=2.10
kafka_scala_0_8_1_1=2.10
kafka_scala_0_8_2_0=2.11
kafka_scala_0_8_2_1=2.11
kafka_scala_0_8_2_2=2.11
kafka_scala_0_9_0_0=2.11
kafka_scala_0_9_0_1=2.11
kafka_scala_0_10_0_0=2.11
kafka_scala_0_10_0_1=2.11
kafka_scala_0_10_1_0=2.11
kafka_scala_4_0_0=2.13
scala_version=$(if $(SCALA_VERSION),$(SCALA_VERSION),$(if $(kafka_scala_$(subst .,_,$(1))),$(kafka_scala_$(subst .,_,$(1))),2.12))

kafka_artifact_name=kafka_$(call scala_version,$(1))-$(1).$(if $(filter 0.8.0,$(1)),tar.gz,tgz)

build-integration: servers/$(KAFKA_VERSION)/kafka-bin

servers/dist:
	mkdir -p servers/dist

servers/dist/kafka_%.tgz servers/dist/kafka_%.tar.gz:
	@echo "Downloading $(@F)"
	wget -nv -P servers/dist/ -N $(DIST_BASE_URL)$(call kafka_artifact_version,$*)/$(@F)

servers/dist/jakarta.xml.bind-api-2.3.3.jar:
	wget -nv -P servers/dist/ -N https://repo1.maven.org/maven2/jakarta/xml/bind/jakarta.xml.bind-api/2.3.3/jakarta.xml.bind-api-2.3.3.jar

# to allow us to derive the prerequisite artifact name from the target name
.SECONDEXPANSION:

servers/%/kafka-bin: servers/dist/$$(call kafka_artifact_name,$$*) | servers/dist
	@echo "Extracting kafka $* binaries from $<"
	if [ -d "$@" ]; then rm -rf $@.bak; mv $@ $@.bak; fi
	mkdir -p $@
	tar xzvf $< -C $@ --strip-components 1
	if [[ "$*" < "1" ]]; then make servers/patch-libs/$*; fi

servers/%/api_versions: servers/$$*/kafka-bin
	KAFKA_VERSION=$* python -m test.integration.fixtures get_api_versions >$@

servers/%/messages: servers/$$*/kafka-bin
	cd servers/$*/ && jar xvf kafka-bin/libs/kafka-clients-$*.jar common/message/
	mv servers/$*/common/message/ servers/$*/messages/
	rmdir servers/$*/common

servers/patch-libs/%: servers/dist/jakarta.xml.bind-api-2.3.3.jar | servers/$$*/kafka-bin
	cp $< servers/$*/kafka-bin/libs/

servers/download/%: servers/dist/$$(call kafka_artifact_name,$$*) | servers/dist ;

# Avoid removing any pattern match targets as intermediates (without this, .tgz artifacts are removed by make after extraction)
.SECONDARY:
