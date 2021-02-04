#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    'appdirs==1.4.3',
    'articlemetaapi==1.26.7',
    'CacheControl==0.12.6',
    'certifi==2020.6.20',
    'chardet==3.0.4',
    'colorama==0.4.3',
    'contextlib2==0.6.0',
    'distlib==0.3.0',
    'distro==1.4.0',
    'html5lib==1.0.1',
    'idna==2.10',
    'ipaddr==2.2.0',
    'legendarium==2.0.6',
    'lockfile==0.12.2',
    'msgpack==0.6.2',
    'mysqlclient==2.0.1',
    'packaging==20.3',
    'pep517==0.8.2',
    'ply==3.11',
    'progress==1.5',
    'pymongo==3.11.1',
    'pyparsing==2.4.6',
    'pytoml==0.1.21',
    'requests==2.24.0',
    'retrying==1.3.3',
    'six==1.14.0',
    'SQLAlchemy==1.3.20',
    'thriftpy2==0.4.12',
    'urllib3==1.25.11',
    'webencodings==0.5.1',
    'xylose==1.35.4'
]


setup(
    name="scielo-usage-counter",
    version='0.1',
    description="The SciELO COUNTER Tools",
    author="SciELO",
    author_email="scielo-dev@googlegroups.com",
    license="BSD",
    url="https://github.com/scieloorg/access-counter",
    keywords='usage access, counter code of practice 5',
    maintainer_email='rafael.pezzuto@gmail.com',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points="""
    [console_scripts]
    initialize_database=proc.initialize_database:main
    create_dictionaries=proc.create_dictionaries:main
    populate_journals=proc.populate_journals:main
    calculate_metrics=proc.calculate_metrics:main
    export_to_database=proc.export_to_database:main
    extract_pretables=proc.extract_pretables:main
    """
)
