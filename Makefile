# Some simple testing tasks (sorry, UNIX only).

FLAGS=
KAFKA_VERSION=0.11.0.0
SCALA_VERSION=2.11

setup:
	pip install -r requirements-dev.txt
	pip install -Ue .

# Test and produce coverage using tox. This is the same as is run on Travis
test36:
	KAFKA_VERSION=$(KAFKA_VERSION) SCALA_VERSION=$(SCALA_VERSION) tox -e py36 -- $(FLAGS)

test27:
	KAFKA_VERSION=$(KAFKA_VERSION) SCALA_VERSION=$(SCALA_VERSION) tox -e py27 -- $(FLAGS)

# Test using py.test directly if you want to use local python. Useful for other
# platforms that require manual installation for C libraries, ie. Windows.
test-local:
	py.test --pylint --pylint-rcfile=pylint.rc --pylint-error-types=EF kafka test

cov-local:
	py.test --pylint --pylint-rcfile=pylint.rc --pylint-error-types=EF --cov=kafka \
		--cov-config=.covrc --cov-report html kafka test
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

.PHONY: all test36 test27 test-local cov-local clean doc
