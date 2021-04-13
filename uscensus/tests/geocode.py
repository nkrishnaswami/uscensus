import csv
import glob
import logging
import os
import os.path

import pandas as pd

from ..geocode.bulk import FilePersister
from ..geocode.bulk import SqlAlchemyPersister
from ..geocode.bulk import CensusBulkGeocoder
from ..geocode.bulk import to_geodataframe


_logger = logging.getLogger(__name__)


def FilePersister_test():
    pers = FilePersister('test/tmp/tmp-{}.csv', 'test/final.csv')
    cols = ['col1', 'col2']
    rows1 = [{'col1': 11, 'col2': 12}]
    rows2 = [{'col1': 21, 'col2': 22}]
    pers.prepare(cols, {'col1': str, 'col2': str})
    assert cols == pers.cols
    pers.persistTemp(rows1)
    pers.persistTemp(rows2)
    files = sorted(glob.glob(pers.temp.format('*')))
    assert 2 == len(files)
    assert os.path.split(pers.temp.format('0000')) == os.path.split(files[0])
    assert os.path.split(pers.temp.format('0001')) == os.path.split(files[1])
    with open(files[0]) as f:
        rdr = csv.reader(f)
        assert ["11", "12"] == next(rdr)
    with open(files[1]) as f:
        rdr = csv.reader(f)
        assert ["21", "22"] == next(rdr)
    df = pers.persistFinal()
    assert 2 == df.shape[0]
    assert ["11", "12"] == df.iloc[0].values.tolist()
    assert ["21", "22"] == df.iloc[1].values.tolist()


def SqlAlchemyPersister_test():
    pers = SqlAlchemyPersister('sqlite://', 'test')
    cols = ['col1', 'col2']
    rows1 = [{'col1': 11, 'col2': 12}]
    rows2 = [{'col1': 21, 'col2': 22}]
    pers.prepare(cols, {'col1': str, 'col2': str})
    assert cols == pers.cols
    pers.persistTemp(rows1)
    pers.persistTemp(rows2)
    df = pers.persistFinal()
    assert 2 == df.shape[0]
    assert ["11", "12"] == df.iloc[0].values.tolist()
    assert ["21", "22"] == df.iloc[1].values.tolist()


def CensusBulkGeocoder_df_test():
    pers = SqlAlchemyPersister('sqlite://', 'test')
    cgc = CensusBulkGeocoder(pers)
    df = pd.DataFrame(
        [['WH000', '1600 Pennsylvania Ave NW',
          'Washington', 'DC', '20500']],
    )
    out = cgc.geocode_df(df, range(5))
    out.set_index('Key', inplace=True)
    row = out.loc['WH000']
    assert 'Match' == row['Match']
    assert 'Exact' == row['Exact']
    assert '1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500' \
        == row['Geo.Address']
    assert '-77.03535,38.898754' == row['Geo.Lon.Lat']
    assert '76225813' == row['Geo.TIGER.LineID']
    assert 'L' == row['Geo.TIGER.Side']
    assert '11' == row['Geo.FIPS.State']
    assert '001' == row['Geo.FIPS.County']
    assert '980000' == row['Geo.Tract']
    assert '1034' == row['Geo.Block']


def CensusBulkGeocoder_rows_test():
    pers = SqlAlchemyPersister('sqlite://', 'test')
    cgc = CensusBulkGeocoder(pers)
    out = cgc.geocode_rows([
        ['WH000', '1600 Pennsylvania Ave NW',
         'Washington', 'DC', '20500']])
    out.set_index('Key', inplace=True)
    row = out.loc['WH000']
    assert 'Match' == row['Match']
    assert 'Exact' == row['Exact']
    assert '1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500' \
        == row['Geo.Address']
    assert '-77.03535,38.898754' == row['Geo.Lon.Lat']
    assert '76225813' == row['Geo.TIGER.LineID']
    assert 'L' == row['Geo.TIGER.Side']
    assert '11' == row['Geo.FIPS.State']
    assert '001' == row['Geo.FIPS.County']
    assert '980000' == row['Geo.Tract']
    assert '1034' == row['Geo.Block']


def CensusBulkGeocoder_cols_test():
    pers = SqlAlchemyPersister('sqlite://', 'test')
    cgc = CensusBulkGeocoder(pers)
    out = cgc.geocode_cols(['WH000'], ['1600 Pennsylvania Ave NW'],
                           ['Washington'], ['DC'], ['20500'])
    out.set_index('Key', inplace=True)
    row = out.loc['WH000']
    assert 'Match' == row['Match']
    assert 'Exact' == row['Exact']
    assert '1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500' \
        == row['Geo.Address']
    assert '-77.03535,38.898754' == row['Geo.Lon.Lat']
    assert '76225813' == row['Geo.TIGER.LineID']
    assert 'L' == row['Geo.TIGER.Side']
    assert '11' == row['Geo.FIPS.State']
    assert '001' == row['Geo.FIPS.County']
    assert '980000' == row['Geo.Tract']
    assert '1034' == row['Geo.Block']
    gout = to_geodataframe(out)
    pt = gout.loc['WH000'].geometry
    assert float('-77.03535') == pt.x
    assert float('38.898754') == pt.y
