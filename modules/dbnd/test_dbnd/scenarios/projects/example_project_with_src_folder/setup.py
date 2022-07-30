#!/usr/bin/env python
# -*- coding: utf-8 -*-


# © Copyright Databand.ai, an IBM Company 2022

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

requirements = ["databand"]


setup(
    name="example_project_with_src_folder",
    version="0.1.0",
    description="Example Project",
    long_description="Example Project",
    author="Evgeny Shulman",
    author_email="evgeny.shulman@databand.ai",
    url="https://bitbucket.org/databand/databand",
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords="databand",
    test_suite="tests",
    tests_require=[],
)
