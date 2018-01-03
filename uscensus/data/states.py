"""Retrieves state abbreviations, names, FIPS and GNS codes as a
pandas DataFrame."""

import pandas as pd


_Columns = [
    'FIPS',
    'USPS',
    'Name',
    'GNSID'
]


def get_state_codes():
    return pd.read_csv(
        'https://www2.census.gov/geo/docs/reference/state.txt',
        delimiter='|',
        skiprows=1,
        names=_Columns
    )
