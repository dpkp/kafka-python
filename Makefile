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
	docker run kafka_python_test /work/run_itest.sh

clean:
	rm -rf kafka-python.egg-info/ .tox/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	docker rmi -f kafka_python_test

docs:
	tox -e docs

.PHONY: docs test all
