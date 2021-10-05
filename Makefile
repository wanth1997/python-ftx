.PHONY: all install test clean

test:
	tox

lint:
	black -l 79 *.py */*.py
