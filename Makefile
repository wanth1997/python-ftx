.PHONY: all install test clean

test:
	tox

lint:
	black -l 120 *.py */*.py
