# pipelinewise-tap-kafka

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-kafka.svg)](https://badge.fury.io/py/pipelinewise-tap-kafka)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-kafka.svg)](https://pypi.org/project/pipelinewise-tap-kafka/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

This is a [Singer](https://singer.io) tap that reads data from Kafka topic and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible target connector.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Kafka](https://transferwise.github.io/pipelinewise/connectors/taps/kafka.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

## Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-tap-kafka
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
```

### Configuration

### Create a config.json

```
{
  "group_id": "1",
  "bootstrap_servers": "foo.com,bar.com",
  "topic": "messages",
  "primary_keys": {
    "id": "$.jsonpath.to.primary_key"
  }
}
```

This tap reads Kafka messages and generating singer compatible SCHEMA and RECORD messages in the following format.

| Property Name               | Description                                                                         |
|-----------------------------|-------------------------------------------------------------------------------------|
| MESSAGE                     | The original Kafka message                                                          |
| DYNAMIC_PRIMARY_KEY(S)      | (Optional) Dynamically added primary key values, extracted from the Kafka message   |

 
### Run the tap in Discovery Mode

```
tap-kafka --config config.json --discover                # Should dump a Catalog to sdtout
tap-kafka --config config.json --discover > catalog.json # Capture the Catalog
```

### Add Metadata to the Catalog

Each entry under the Catalog's "stream" key will need the following metadata:

```
{
  "streams": [
    {
      "stream_name": "my_topic"
      "metadata": [{
        "breadcrumb": [],
        "metadata": {
          "selected": true,
        }
      }]
    }
  ]
}
```

### Run the tap in Sync Mode

```
tap-kafka --config config.json --properties catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter to the tap for the next sync.

## To run tests:

1. Install python test dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .[test]
```

2. To run tests:
```
  pytest tests
```

## To run pylint:

1. Install python dependencies and run python linter
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .[test]
  pylint tap_kafka -d C,W,unexpected-keyword-arg,duplicate-code
```
