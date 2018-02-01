#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="uscensus",
    version="0.1.9",
    packages=find_packages(),
    description="US Census API discovery wrappers",
    long_description="""This module fetches metadata for the Census Data API to make it easier to find relevant data sets and variables to query, and returns data as a Pandas dataframe.""",
    license="Apache Software License 2.0",
    keywords="census demographics economics data api discovery",
    url="https://github.com/nkrishnaswami/uscensus",

    test_suite='nose.collector',
    tests_require=['nose'],

    install_requires=[
        'docutils>=0.3',
        'python-dateutil',
        'fiona',
        'geopandas',
        'gevent',
        'grequests',
        'numpy',
        'pandas',
        'pymongo',
        'pytz',
        'requests',
        'shapely',
        'sqlalchemy',
        'whoosh',
    ],

    author="Natarajan Krishnaswami",
    author_email="nkrish@acm.org",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Sociology',
        'Topic :: Scientific/Engineering :: GIS',
    ],
)
