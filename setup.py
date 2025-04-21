#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-ydb"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "0.0.0"
description = """The YDB adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Oleg Ovcharuk",
    author_email="vgvoleg@gmail.com",
    url="If you have already made a github repo to tie the project to place it here, otherwise update in setup.py later.",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=0.0.0.",
        "dbt-common<1.0"
        "dbt-adapter~=0.1.0a2"
    ],
)
