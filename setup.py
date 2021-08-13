#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-kafka',
      version='4.0.1',
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
          'kafka-python==2.0.1',
          'pipelinewise-singer-python==1.*',
          'dpath==2.0.1',
          'filelock==3.0.12'
      ],
      extras_require={
          "test": [
              "pytest==5.0.1",
              "pylint==2.4.2",
              'pytest-cov==2.10.1'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-kafka=tap_kafka:main
      ''',
      packages=['tap_kafka']
)
