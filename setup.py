#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-kafka',
      version='8.0.0',
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
          'confluent-kafka[protobuf]==1.9.2',
          'grpcio-tools==1.51.1'
      ],
      extras_require={
          'test': [
              'pytest==7.2.0',
              'pylint==2.15.7',
              'pytest-cov==4.0.0'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-kafka=tap_kafka:main
      ''',
      packages=['tap_kafka', 'tap_kafka.serialization']
)
