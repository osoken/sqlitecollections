from setuptools import setup

from sqlitecollections import (
    __author__,
    __description__,
    __email__,
    __long_description__,
    __package_name__,
    __version__,
)

setup(
    name=__package_name__,
    version=__version__,
    author=__author__,
    author_email=__email__,
    license="MIT",
    url="https://github.com/osoken/sqlitecollections",
    description=__description__,
    long_description=__long_description__,
    packages=[__package_name__],
    install_requires=[],
    extras_require={
        "dev": ["flake8", "pytest", "black", "mypy", "tox", "isort"],
        "docs": ["mkdocs", "mkdocs-material"],
    },
)
