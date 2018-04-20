#!/usr/bin/env python3
from setuptools import setup
import os

setup(
    name='coal',
    version=1.0,
    author='Ian Marlier',
    author_email='imarlier@wikimedia.org',
    url='https://wikitech.wikimedia.org/wiki/Performance.wikimedia.org#Coal',
    license='Apache 2.0',
    description='Performance team website and metrics processing',
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    packages=[
        'coal'
    ],
    install_requires=[
        'kafka-python',
        'python-dateutil',
        'whisper',
        'flask',
        'numpy',
        'pytz',
        'python-etcd'
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
    entry_points={
        'console_scripts': [
            'coal = coal:main',
            'coal-web = coal.coal_web:main'
        ],
    }
)
