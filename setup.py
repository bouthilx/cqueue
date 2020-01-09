#!/usr/bin/env python
from setuptools import setup
import subprocess
import os


if __name__ == '__main__':
    _base = os.path.dirname(os.path.realpath(__file__))
    subprocess.call(f'./{_base}/install_cockroach.sh.sh', shell=True)

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
        data_files=[
            ('cqueue', ['backends/bin/cockroach'])
        ],
        setup_requires=['setuptools'],
        tests_require=['pytest', 'flake8', 'codecov', 'pytest-cov'],
    )
