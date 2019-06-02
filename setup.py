#!/usr/bin/env python

from setuptools import setup

setup(name='pipelinewise-tap-kafka',
      version='1.0.0',
      description='Singer.io tap for extracting data from Kafka topic - PipelineWise compatible',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'kafka-python',
          'singer-python==5.2.0',
          'requests==2.20.0',
	      'strict-rfc3339==0.7',
	      'nose==1.3.7',
          'jsonschema==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          tap-kafka=tap_kafka:main
      ''',
      packages=['tap_kafka']
)
