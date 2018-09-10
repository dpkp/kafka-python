.DELETE_ON_ERROR:

all: test itest

test:
	tox -e py27
	tox -e py35

unit_test_docker:
	docker build -t kafka_python_test .
	docker run kafka_python_test /work/run_utest.sh

itest:
	docker build -t kafka_python_test .
#   travis build passes because ipv6 is disabled there
#   it passes the integration test locally if we disable ipv6 here
	docker run --sysctl net.ipv6.conf.all.disable_ipv6=1 kafka_python_test /work/run_itest.sh

docs:
	tox -e docs


FLAGS=
KAFKA_VERSION=1.0.1
SCALA_VERSION=2.12

setup:
	pip install -r requirements-dev.txt
	pip install -Ue .

servers/$(KAFKA_VERSION)/kafka-bin:
	KAFKA_VERSION=$(KAFKA_VERSION) SCALA_VERSION=$(SCALA_VERSION) ./build_integration.sh

build-integration: servers/$(KAFKA_VERSION)/kafka-bin

# Test and produce coverage using tox. This is the same as is run on Travis
test36: build-integration
	KAFKA_VERSION=$(KAFKA_VERSION) SCALA_VERSION=$(SCALA_VERSION) tox -e py36 -- $(FLAGS)

test27: build-integration
	KAFKA_VERSION=$(KAFKA_VERSION) SCALA_VERSION=$(SCALA_VERSION) tox -e py27 -- $(FLAGS)

# Test using py.test directly if you want to use local python. Useful for other
# platforms that require manual installation for C libraries, ie. Windows.
test-local: build-integration
	KAFKA_VERSION=$(KAFKA_VERSION) SCALA_VERSION=$(SCALA_VERSION) py.test \
		--pylint --pylint-rcfile=pylint.rc --pylint-error-types=EF $(FLAGS) kafka test

cov-local: build-integration
	KAFKA_VERSION=$(KAFKA_VERSION) SCALA_VERSION=$(SCALA_VERSION) py.test \
		--pylint --pylint-rcfile=pylint.rc --pylint-error-types=EF --cov=kafka \
		--cov-config=.covrc --cov-report html $(FLAGS) kafka test
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
	rm -rf kafka-python.egg-info/ .tox/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	docker rmi -f kafka_python_test

.PHONY: all test36 test27 test-local cov-local clean doc docs test
