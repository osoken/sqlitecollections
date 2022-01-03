import os

from setuptools import setup

from scbenchmarker import (
    __author__,
    __description__,
    __email__,
    __package_name__,
    __version__,
)

root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

setup(
    name=__package_name__,
    version=__version__,
    author=__author__,
    author_email=__email__,
    license="MIT",
    url="https://github.com/osoken/sqlitecollections/docs/benchmarks",
    description=__description__,
    long_description=__description__,
    packages=[__package_name__],
    install_requires=[
        "memory_profiler",
        "jinja2",
        f'sqlitecollections @ file://{root_dir}',
    ],
)
