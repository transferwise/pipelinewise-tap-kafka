ZOOKEEPER_CLIENT_PORT = 2181
KAFKA_PORT = 29092

venv:
	python3 -m venv venv ;\
	. venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

clean_containers:
	docker-compose kill
	docker-compose rm -f

wait-for-zookeeper:
	docker run --rm --network 'pipelinewise_tap_kafka_network' busybox /bin/sh -c "until nc -z zookeeper ${ZOOKEEPER_CLIENT_PORT}; do sleep 1; echo 'Waiting for Zookeeper to come up...'; done"

wait-for-kafka:
	docker run --rm --network 'pipelinewise_tap_kafka_network' busybox /bin/sh -c "until nc -z kafka ${KAFKA_PORT}; do sleep 1; echo 'Waiting for Kafka to come up...'; done"

start_containers: clean_containers
	docker-compose up -d
	make wait-for-zookeeper
	make wait-for-kafka

lint:
	. ./venv/bin/activate ;\
	pylint tap_kafka -d C,W,unexpected-keyword-arg,duplicate-code

unit_test:
	. ./venv/bin/activate ;\
	pytest --cov=tap_kafka  --cov-fail-under=73 tests/unit -v

integration_test: start_containers
	. ./venv/bin/activate ;\
	TAP_KAFKA_BOOTSTRAP_SERVERS=localhost:${KAFKA_PORT} pytest --cov=tap_kafka  --cov-fail-under=75 tests/integration -v
	make clean_containers