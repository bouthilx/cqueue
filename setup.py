#!/usr/bin/env python
from setuptools import setup

if __name__ == '__main__':
    setup(
        name='cqueue',
        version='0.0.0',
        description='Message Queue Primitives',
        author='Pierre Delaunay',
        packages=[
            'cqueue',
            'cqueue.backends',
        ],
        install_requires=[
            'dataclasses',
            'typing',
        ],
        setup_requires=['setuptools'],
        tests_require=['pytest', 'flake8', 'codecov', 'pytest-cov'],
    )
