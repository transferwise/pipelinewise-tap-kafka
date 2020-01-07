#!/usr/bin/env python

from setuptools import setup

setup(name='pipelinewise-tap-kafka',
      version='2.0.0',
      description='Singer.io tap for extracting data from Kafka topic - PipelineWise compatible',
      author='Stitch',
      url='https://singer.io',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      install_requires=[
          'kafka-python',
          'singer-python==5.2.0',
          'jsonpath_ng==1.4.3'
      ],
      extras_require={
          "test": [
              "pytest==5.0.1",
              "nose==1.3.7",
              "pylint==2.4.2"
          ]
      },
      entry_points='''
          [console_scripts]
          tap-kafka=tap_kafka:main
      ''',
      packages=['tap_kafka']
)
