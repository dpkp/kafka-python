.PHONY: clean env/bin/activate stats

PWD=`pwd`
ENV = env
PYTHON = exec python

clean:
	rm -rf build/
	rm -rf dist/

package:
	$(PYTHON) setup.py bdist_egg
	$(PYTHON) setup.py sdist 
