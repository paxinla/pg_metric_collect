init:
	test -d ENV || { virtualenv --no-site-packages ENV ;}
	ENV/bin/pip install -r requirements.txt

clean:
	-rm -r build
	-rm -r dist
	-rm main.spec

PYVER = $(shell ENV/bin/python --version | cut -d'.' -f1-2 | tr 'A-Z' 'a-z' | sed 's/ //g')
build: clean
	test -f ENV/lib/$(PYVER)/site-packages/google/__init__.py || touch ENV/lib/$(PYVER)/site-packages/google/__init__.py
	ENV/bin/pyinstaller --onefile pg_metric_collect/main.py
