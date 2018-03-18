#!/usr/bin/env python
# coding: utf-8

from setuptools import setup

import nexus


setup(
    name='nexus-rpc',
    version=nexus.__version__,
    packages=['nexus', 'nexus.platform'],
    author='JZQT',
    author_email='561484726@qq.com',
    url="https://github.com/JZQT/nexus-rpc",
    description="Nexus is a http-based rpc framework.",
    license='MIT',
    install_requires=['aiohttp', 'thriftpy'],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ]
)
