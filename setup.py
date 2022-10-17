#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-kafka',
      version='7.1.1',
      description='Singer.io tap for extracting data from Kafka topic - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='TransferWise',
      url='https://github.com/transferwise/pipelinewise-tap-kafka',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      install_requires=[
          'pipelinewise-singer-python==2.*',
          'dpath==2.0.6',
          'confluent-kafka[protobuf]==1.9.0',
          'grpcio-tools==1.44.0'
      ],
      extras_require={
          'test': [
              'pytest==7.0.1',
              'pylint==2.12.2',
              'pytest-cov==3.0.0'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-kafka=tap_kafka:main
      ''',
      packages=['tap_kafka', 'tap_kafka.serialization']
)
