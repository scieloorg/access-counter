#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    'articlemetaapi==1.26.7',
    'certifi==2021.5.30',
    'chardet==4.0.0',
    'dateutils==0.6.12',
    'greenlet==1.1.0',
    'idna==2.10',
    'legendarium==2.0.6',
    'lxml==4.6.3',
    'mysqlclient==2.0.3',
    'ply==3.11',
    'pymongo==3.11.4',
    'python-dateutil==2.8.1',
    'pytz==2021.1',
    'requests==2.25.1',
    'Sickle==0.7.0',
    'six==1.16.0',
    'SQLAlchemy==1.4.19',
    'thriftpy2==0.4.14',
    'urllib3==1.26.6',
    'xylose==1.35.4',
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
    collect_opac_dictionary=proc.collect_opac_dictionary:main
    """
)
