"""Retrieves state abbreviations, names, FIPS and GNS codes as a
pandas DataFrame."""

from functools import cache
import re
from tempfile import NamedTemporaryFile
from typing import Optional, Union
import zipfile

import geopandas as gpd
import pandas as pd
import httpx

from ..util.errors import CensusError
from ..util.webcache import fetch


BOUNDARY_RESOLUTIONS = ('20m', '5m', '500k')


@cache
def _get_boundaries(
        session: httpx.Client,
        resolution: str,
        vintage: str,
        layer: str) -> gpd.GeoDataFrame:
    """Retrieves a cartographic boundary file geodatabase, and
    extracts one layer as GeoDataFrame."""
    if resolution not in BOUNDARY_RESOLUTIONS:
        raise ValueError(f'Invalid resolution {resolution}')
    basename = f'cb_{vintage}_us_all_{resolution}'
    url = f'https://www2.census.gov/geo//tiger/GENZ2021/gdb/{basename}.zip'
    r = fetch(url, session)
    if not r:
        raise CensusError(f'Unable to retrieve census data from {url}')
    # Save the zip data to a temporary file. It needs to be in the FS
    # so we can refer to its name.
    with NamedTemporaryFile(suffix='.zip') as tempzip:
        tempzip.write(r.content)
        # Read the zip file index to find the first directory with
        # '.gdb/' suffix.
        with zipfile.ZipFile(tempzip.name, 'r') as zf:
            gdbname = None
            for name in zf.namelist():
                if name.endswith('.gdb/'):
                    gdbname = name[:-1]
                    break
            if gdbname is None:
                raise ValueError('Geodatabase not found in downloaded data!')
        return gpd.read_file(
            f'/vsizip/{tempzip.name}/{gdbname}',
            layer=layer)


def get_county_boundaries(
        session: httpx.Client,
        *,
        state: Optional[str] = None,
        vintage: Union[str, int] = 2021,
        resolution: str = '20m') -> gpd.GeoDataFrame:
    boundaries = _get_boundaries(session, resolution, vintage,
                                 f'cb_{vintage}_us_county_{resolution}')
    if state:
        if re.match(r'^[A-Z]{2}$', state):
            boundaries = boundaries[boundaries['STUSPS'] == state]
        elif re.match(r'^[0-9]{2}$', state):
            boundaries = boundaries[boundaries['STATEFP'] == state]
        else:
            boundaries = boundaries[boundaries['STATENAME'] == state]
    return boundaries


def get_state_boundaries(
        session: httpx.Client,
        *,
        state: Optional[str] = None,
        vintage: Union[str, int] = 2021,
        resolution: str = '20m') -> gpd.GeoDataFrame:
    boundaries = _get_boundaries(session, resolution, vintage,
                                 f'cb_{vintage}_us_state_{resolution}')
    if state:
        if re.match(r'^[A-Z]{2}$', state):
            boundaries = boundaries[boundaries['STUSPS'] == state]
        elif re.match(r'^[0-9]{2}$', state):
            boundaries = boundaries[boundaries['STATEFP'] == state]
        elif re.match(r'^[0-9]{8}$', state):
            boundaries = boundaries[boundaries['STATENS'] == state]
        else:
            boundaries = boundaries[boundaries['NAME'] == state]
    return boundaries


def get_state_codes(
        session: httpx.Client,
        *,
        vintage: Union[str, int] = 2021) -> pd.DataFrame:
    """Return a DataFrame with codes and names for US states and
    territories, for vintage.
    """
    boundaries = get_state_boundaries(session, vintage=vintage)
    return boundaries[['STATEFP', 'STATENS', 'STUSPS', 'NAME']]


def get_county_codes(
        session: httpx.Client,
        *,
        vintage: Union[str, int] = 2021,
        state: Optional[str] = None) -> pd.DataFrame:
    """Return a DataFrame with codes and names for US states and
    territories, for vintage and state, if specified.
    """
    boundaries = get_county_boundaries(session, vintage=vintage, state=state)
    return boundaries[['GEOID', 'STATEFP', 'STUSPS', 'STATE_NAME', 'COUNTYFP',
                       'COUNTYNS', 'NAME']]
