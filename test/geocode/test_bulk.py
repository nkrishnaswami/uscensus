import csv
import glob
import logging
import os
import os.path

import pandas as pd
import pytest

from uscensus.geocode.bulk import (
    CensusBulkGeocoder,
    FilePersister,
    SqlAlchemyPersister,
    to_geodataframe,
)

_logger = logging.getLogger(__name__)


def test_FilePersister():
    pers = FilePersister('test/tmp/tmp-{}.csv', 'test/final.csv')
    cols = ['col1', 'col2']
    rows1 = [{'col1': 11, 'col2': 12}]
    rows2 = [{'col1': 21, 'col2': 22}]
    pers.prepare(cols, {'col1': str, 'col2': str})
    assert cols == pers.cols
    pers.persistTemp(rows1)
    pers.persistTemp(rows2)
    files = sorted(glob.glob(pers.temp.format('*')))
    assert len(files) == 2
    assert os.path.split(pers.temp.format('0000')) == os.path.split(files[0])
    assert os.path.split(pers.temp.format('0001')) == os.path.split(files[1])
    with open(files[0]) as f:
        rdr = csv.reader(f)
        assert next(rdr) == ['11', '12']
    with open(files[1]) as f:
        rdr = csv.reader(f)
        assert next(rdr) == ['21', '22']
    df = pers.persistFinal()
    assert df.shape[0] == 2
    assert df.iloc[0].to_numpy().tolist() == ['11', '12']
    assert df.iloc[1].to_numpy().tolist() == ['21', '22']


def test_SqlAlchemyPersister():
    pers = SqlAlchemyPersister('sqlite://', 'test')
    cols = ['col1', 'col2']
    rows1 = [{'col1': 11, 'col2': 12}]
    rows2 = [{'col1': 21, 'col2': 22}]
    pers.prepare(cols, {'col1': str, 'col2': str})
    assert cols == pers.cols
    pers.persistTemp(rows1)
    pers.persistTemp(rows2)
    df = pers.persistFinal()
    assert df.shape[0] == 2
    assert df.iloc[0].to_numpy().tolist() == ['11', '12']
    assert df.iloc[1].to_numpy().tolist() == ['21', '22']


def test_CensusBulkGeocoder_df():
    pers = SqlAlchemyPersister('sqlite://', 'test')
    cgc = CensusBulkGeocoder(pers)
    df = pd.DataFrame(
        [['WH000', '1600 Pennsylvania Ave NW',
          'Washington', 'DC', '20500']],
    )
    out = cgc.geocode_df(df, range(5))
    out = out.set_index('Key')
    row = out.loc['WH000']
    assert row['Match'] == 'Match'
    assert row['Exact'] == 'Exact'
    assert row['Geo.Address'] \
        == '1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500'
    lon, lat = map(float, row['Geo.Lon.Lat'].split(','))
    assert pytest.approx(-77.035, 0.0005) == lon
    assert pytest.approx(38.899, 0.0005) == lat
    assert row['Geo.TIGER.LineID'] == '76225813'
    assert row['Geo.TIGER.Side'] == 'L'
    assert row['Geo.FIPS.State'] == '11'
    assert row['Geo.FIPS.County'] == '001'
    assert row['Geo.Tract'] == '980000'
    assert row['Geo.Block'] == '1034'


def test_CensusBulkGeocoder_rows():
    pers = SqlAlchemyPersister('sqlite://', 'test')
    cgc = CensusBulkGeocoder(pers)
    out = cgc.geocode_rows([
        ['WH000', '1600 Pennsylvania Ave NW',
         'Washington', 'DC', '20500']])
    out = out.set_index('Key')
    row = out.loc['WH000']
    assert row['Match'] == 'Match'
    assert row['Exact'] == 'Exact'
    assert row['Geo.Address'] \
        == '1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500'
    lon, lat = map(float, row['Geo.Lon.Lat'].split(','))
    assert pytest.approx(-77.035, 0.0005) == lon
    assert pytest.approx(38.899, 0.0005) == lat
    assert row['Geo.TIGER.LineID'] == '76225813'
    assert row['Geo.TIGER.Side'] == 'L'
    assert row['Geo.FIPS.State'] == '11'
    assert row['Geo.FIPS.County'] == '001'
    assert row['Geo.Tract'] == '980000'
    assert row['Geo.Block'] == '1034'


def test_CensusBulkGeocoder_cols():
    pers = SqlAlchemyPersister('sqlite://', 'test')
    cgc = CensusBulkGeocoder(pers)
    out = cgc.geocode_cols(['WH000'], ['1600 Pennsylvania Ave NW'],
                           ['Washington'], ['DC'], ['20500'])
    out = out.set_index('Key')
    row = out.loc['WH000']
    assert row['Match'] == 'Match'
    assert row['Exact'] == 'Exact'
    assert row['Geo.Address'] \
        == '1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500'
    lon, lat = map(float, row['Geo.Lon.Lat'].split(','))
    assert pytest.approx(-77.035, 0.0005) == lon
    assert pytest.approx(38.899, 0.0005) == lat
    assert row['Geo.TIGER.LineID'] == '76225813'
    assert row['Geo.TIGER.Side'] == 'L'
    assert row['Geo.FIPS.State'] == '11'
    assert row['Geo.FIPS.County'] == '001'
    assert row['Geo.Tract'] == '980000'
    assert row['Geo.Block'] == '1034'
    gout = to_geodataframe(out)
    pt = gout.loc['WH000'].geometry
    assert pytest.approx(-77.035, 0.0005) == pt.x
    assert pytest.approx(38.899, 0.0005) == pt.y
