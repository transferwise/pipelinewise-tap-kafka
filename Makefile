venv:
	python3 -m venv venv
	. ./venv/bin/activate
	pip install --upgrade pip setuptools wheel
	pip install -e .[test]

lint:
	. ./venv/bin/activate
	pylint tap_kafka -d C,W,unexpected-keyword-arg,duplicate-code

test:
	. ./venv/bin/activate
	pytest --cov=tap_kafka  --cov-fail-under=73 tests -v
