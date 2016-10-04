.DELETE_ON_ERROR:

all: test itest

test:
	tox -e py27
	tox -e py35

itest:
	docker build -t kafka_python_test .
	docker run -i -t kafka_python_test /bin/bash -c "export KAFKA_VERSION='0.9.0.1'; ./build_integration.sh; \
	    						tox -e py27; tox -e py35; tox -e pypy; \
	    						export KAFKA_VERSION='0.10.0.0'; ./build_integration.sh; \
							tox -e py27; tox -e py35; tox -e pypy; exit $?"

clean:
	rm -rf kafka-python.egg-info/ .tox/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	docker rmi -f kafka_python_test

docs:
	tox -e docs

.PHONY: docs
