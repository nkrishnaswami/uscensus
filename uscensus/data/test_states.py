import pytest
import httpx
from httpx_caching import CachingClient

from .states import (get_county_boundaries, get_county_codes,
                     get_state_boundaries, get_state_codes)
from ..util.datastores import SqlAlchemyDataStore


@pytest.fixture
def client():
    return CachingClient(
        httpx.Client(),
        cache=SqlAlchemyDataStore('sqlite:///testing.db'))


def test_get_county_boundaries_no_state_bad_res(client):
    with pytest.raises(ValueError):
        get_county_boundaries(client, resolution='20k')


def test_get_county_boundaries_no_state(client):
    df = get_county_boundaries(client)
    df = df.set_index('GEOID').sort_index()
    assert(len(df)) > 3000
    assert '01001' in df.index
    autauga = df.loc['01001']
    assert autauga.STATE_NAME == 'Alabama'
    assert autauga.STATEFP == '01'
    assert autauga.STUSPS == 'AL'
    assert autauga.NAME == 'Autauga'
    assert autauga.COUNTYFP == '001'


def test_get_county_boundaries_usps_state(client):
    df = get_county_boundaries(client, state='AL')
    df = df.set_index('GEOID').sort_index()
    assert(len(df)) < 100
    assert '01001' in df.index
    autauga = df.loc['01001']
    assert autauga.STATE_NAME == 'Alabama'
    assert autauga.STATEFP == '01'
    assert autauga.STUSPS == 'AL'
    assert autauga.NAME == 'Autauga'
    assert autauga.COUNTYFP == '001'


def test_get_county_boundaries_fips_state(client):
    df = get_county_boundaries(client, state='01')
    df = df.set_index('GEOID').sort_index()
    assert(len(df)) < 100
    assert '01001' in df.index
    autauga = df.loc['01001']
    assert autauga.STATE_NAME == 'Alabama'
    assert autauga.STATEFP == '01'
    assert autauga.STUSPS == 'AL'
    assert autauga.NAME == 'Autauga'
    assert autauga.COUNTYFP == '001'

def test_get_county_boundaries_no_state(client):
    df = get_county_boundaries(client)
    df = df.set_index('GEOID').sort_index()
    assert '01001' in df.index
    autauga = df.loc['01001']
    assert autauga.STATE_NAME == 'Alabama'
    assert autauga.STATEFP == '01'
    assert autauga.STUSPS == 'AL'
    assert autauga.NAME == 'Autauga'
    assert autauga.COUNTYFP == '001'


def test_get_county_boundaries_bad_fips_state(client):
    df = get_county_boundaries(client, state='99')
    assert len(df.index) == 0


def test_get_county_boundaries_bad_usps_state(client):
    df = get_county_boundaries(client, state='ZZ')
    assert len(df.index) == 0


def test_get_state_boundaries_no_state_bad_res(client):
    with pytest.raises(ValueError):
        get_state_boundaries(client, resolution='20k')


def test_get_state_boundaries_no_state(client):
    df = get_state_boundaries(client)
    df = df.set_index('GEOID').sort_index()
    assert '01' in df.index
    alabama = df.loc['01']
    assert alabama.NAME == 'Alabama'
    assert alabama.STATEFP == '01'
    assert alabama.STUSPS == 'AL'


def test_get_state_boundaries_bad_fips_state(client):
    df = get_state_boundaries(client, state='99')
    assert len(df.index) == 0


def test_get_state_boundaries_bad_usps_state(client):
    df = get_state_boundaries(client, state='ZZ')
    assert len(df.index) == 0


def test_get_county_codes_no_state(client):
    df = get_county_codes(client)
    df = df.set_index(['STATEFP', 'COUNTYFP']).sort_index()
    assert(len(df)) > 3000
    assert ('01', '001') in df.index
    autauga = df.loc[('01', '001')]
    assert autauga.STATE_NAME == 'Alabama'
    assert autauga.STUSPS == 'AL'
    assert autauga.NAME == 'Autauga'


def test_get_county_codes_usps_state(client):
    df = get_county_codes(client, state='AL')
    df = df.set_index(['STATEFP', 'COUNTYFP']).sort_index()
    assert(len(df)) < 100
    assert ('01', '001') in df.index
    autauga = df.loc[('01', '001')]
    assert autauga.STATE_NAME == 'Alabama'
    assert autauga.STUSPS == 'AL'
    assert autauga.NAME == 'Autauga'


def test_get_county_codes_fips_state(client):
    df = get_county_codes(client, state='01')
    df = df.set_index(['STATEFP', 'COUNTYFP']).sort_index()
    assert(len(df)) < 100
    assert ('01', '001') in df.index
    autauga = df.loc[('01', '001')]
    assert autauga.STATE_NAME == 'Alabama'
    assert autauga.STUSPS == 'AL'
    assert autauga.NAME == 'Autauga'


def test_get_county_codes_bad_fips_state(client):
    df = get_county_codes(client, state='99')
    assert len(df.index) == 0


def test_get_county_codes_bad_usps_state(client):
    df = get_county_codes(client, state='ZZ')
    assert len(df.index) == 0


def test_get_state_codes(client):
    df = get_state_codes(client)
    df = df.set_index('STATEFP').sort_index()
    assert '01' in df.index
    alabama = df.loc['01']
    assert alabama.NAME == 'Alabama'
    assert alabama.STUSPS == 'AL'

