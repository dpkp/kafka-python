# Some simple testing tasks

SHELL = bash

export KAFKA_VERSION ?= 4.2.0
DIST_BASE_URL ?= https://downloads.apache.org/kafka/
ARCHIVE_BASE_URL = https://archive.apache.org/dist/kafka/

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

test-coverage: build-integration
	pytest --cov=kafka --cov-report html test
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

stubs:
	python -m kafka.protocol.generate_stubs

check-stubs:
	python -m kafka.protocol.generate_stubs --check

bench-protocol:
	python kafka/benchmarks/protocol_old_vs_new.py

bench-protocol-fast:
	python kafka/benchmarks/protocol_old_vs_new.py --fast

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: all test test-local test-coverage clean doc dist publish stubs check-stubs bench-protocol bench-protocol-fast

kafka_artifact_version=$(lastword $(subst -, ,$(1)))

# Version comparison: returns "yes" if $(1) >= $(2) (using sort -V)
version_ge=$(shell printf '%s\n%s\n' '$(2)' '$(1)' | sort -V | head -n1 | grep -qx '$(2)' && echo yes)

# Determine scala version based on kafka version thresholds
# 0.8.0 => 2.8.0, >=0.8.1 => 2.10, >=0.8.2 => 2.11, >=0.11 => 2.12, >=4.0 => 2.13
scala_version=$(if $(SCALA_VERSION),$(SCALA_VERSION),$(if $(call version_ge,$(1),4.0),2.13,$(if $(call version_ge,$(1),0.11),2.12,$(if $(call version_ge,$(1),0.8.2),2.11,$(if $(call version_ge,$(1),0.8.1),2.10,2.8.0)))))

kafka_artifact_name=kafka_$(call scala_version,$(1))-$(1).$(if $(filter 0.8.0,$(1)),tar.gz,tgz)

build-integration: servers/$(KAFKA_VERSION)/kafka-bin

servers/dist:
	mkdir -p servers/dist

servers/dist/kafka_%.tgz servers/dist/kafka_%.tar.gz:
	$(eval artifact_path=$(call kafka_artifact_version,$*)/$(@F))
	@echo "Downloading $(@F)"
	wget -nv -P servers/dist/ -N $(DIST_BASE_URL)$(artifact_path) || \
	wget -nv -P servers/dist/ -N $(ARCHIVE_BASE_URL)$(artifact_path)

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
	if [[ -f "/etc/NIXOS" ]]; then make servers/patch-nixos-shebang/$*; fi

servers/%/api_versions: servers/$$*/kafka-bin
	KAFKA_VERSION=$* python -m test.integration.fixtures get_api_versions >$@

servers/%/messages: servers/$$*/kafka-bin
	cd servers/$*/ && jar xvf kafka-bin/libs/kafka-clients-$*.jar common/message/
	mv servers/$*/common/message/ servers/$*/messages/
	rmdir servers/$*/common

servers/patch-libs/%: servers/dist/jakarta.xml.bind-api-2.3.3.jar | servers/$$*/kafka-bin
	cp $< servers/$*/kafka-bin/libs/

servers/patch-nixos-shebang/%:
	for f in $$(ls servers/$*/kafka-bin/bin/*.sh); do if (head -1 $$f | grep '#!/bin/bash' >/dev/null); then echo $$f; sed -i '1s|/bin/bash|/run/current-system/sw/bin/bash|' $$f; fi; done;

servers/download/%: servers/dist/$$(call kafka_artifact_name,$$*) | servers/dist ;

# Avoid removing any pattern match targets as intermediates (without this, .tgz artifacts are removed by make after extraction)
.SECONDARY:
