VENV_DIR = ./venv
ZOOKEEPER_CLIENT_PORT = 2181
KAFKA_PORT = 29092

.wait-for-containers:
	docker run --rm --network 'pipelinewise_tap_kafka_network' busybox /bin/sh -c "until nc -z zookeeper ${ZOOKEEPER_CLIENT_PORT}; do sleep 1; echo 'Waiting for Zookeeper to come up...'; done"
	docker run --rm --network 'pipelinewise_tap_kafka_network' busybox /bin/sh -c "until nc -z kafka ${KAFKA_PORT}; do sleep 1; echo 'Waiting for Kafka to come up...'; done"

.run_pytest_unit:
	$(VENV_DIR)/bin/pytest --cov=tap_kafka  --cov-fail-under=73 tests/unit -v

.run_pytest_integration:
	TAP_KAFKA_BOOTSTRAP_SERVERS=localhost:${KAFKA_PORT} $(VENV_DIR)/bin/pytest --cov=tap_kafka  --cov-fail-under=75 tests/integration -v

venv:
	python3 -m venv $(VENV_DIR) ;\
	. $(VENV_DIR)/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

start_containers: clean_containers
	docker-compose up -d
	make .wait-for-containers

clean_containers:
	docker-compose kill
	docker-compose rm -f

lint: venv
	$(VENV_DIR)/bin/pylint tap_kafka -d C,W,unexpected-keyword-arg,duplicate-code

unit_test: venv .run_pytest_unit

integration_test: venv start_containers .run_pytest_integration clean_containers
