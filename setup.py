"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the dbsql_query_logger project.
"""
from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import datetime
import dbsql_query_logger

setup(
    name="dbsql_query_logger",
    # We use timestamp as Local version identifier (https://peps.python.org/pep-0440/#local-version-identifiers.)
    # to ensure that changes to wheel package are picked up when used on all-purpose clusters
    version=dbsql_query_logger.__version__ + "+" + datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
    url="https://github.com/chris-koester-db/dbsql_query_logger",
    author="chris.koester@databricks.com",
    description="wheel file based on dbsql_query_logger/src",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={
        "packages": [
            "main=dbsql_query_logger.main:main"
        ]
    },
    install_requires=[
        # Dependencies in case the output wheel file is used as a library dependency.
        # For defining dependencies, when this package is used in Databricks, see:
        # https://docs.databricks.com/dev-tools/bundles/library-dependencies.html
        "setuptools",
        "databricks-sdk == 0.25.1"
    ],
)
