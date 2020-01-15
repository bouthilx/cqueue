travis-install:
	pip install -e .
	pip install -r requirements.txt
	pip install -r tests/requirements.txt

travis-unit:
	COVERAGE_FILE=.coverage.unit coverage run --parallel-mode -m pytest --cov=cqueue tests

travis-end:
	coverage combine
	coverage report -m
	coverage xml
	codecov
