"""Bulk geocoding API wrapper.

Attributes:
    CENSUS_GEO_COLNAMES: column names in output from geocoding API.
"""
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import csv
import fiona
from io import StringIO
from itertools import islice
import numpy as np
import geopandas as gpd
from gevent.pool import Pool
import glob
import grequests
import os
import os.path
import pandas as pd
import shapely
import sqlalchemy

CENSUS_GEO_COLNAMES = [
    'Key',
    'In.Address',
    'Match',
    'Exact',
    'Geo.Address',
    'Geo.Lon.Lat',
    'Geo.TIGER.LineID',
    'Geo.TIGER.Side',
    'Geo.FIPS.State',
    'Geo.FIPS.County',
    'Geo.Tract',
    'Geo.Block',
]

CENSUS_GEO_DTYPES = {
    'Key': str,
    'Geo.TIGER.LineID': str,
    'Geo.FIPS.State': str,
    'Geo.FIPS.County': str,
    'Geo.Tract': str,
    'Geo.Block': str,
}


def chunker(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(islice(iterable, n)), [])


class FilePersister(object):
    """Saves progress to files."""
    def __init__(self, tempOut, finalOut):
        """Arguments:
          * tempOut: filename template with one positional parameter
            for temporary files.
          * finalOut: filename for final CSV output.
        """
        self.temp = tempOut
        self.cols = None
        self.final = finalOut
        self.dtypes = None
        self.idx = 0
        try:
            os.makedirs(os.path.dirname(self.temp))
        except FileExistsError:
            pass
        try:
            os.makedirs(os.path.dirname(self.final))
        except FileExistsError:
            pass

    def prepare(self, cols, dtypes):
        self.cols = cols
        self.dtypes = dtypes

    def persistTemp(self, rows):
        with open(self.temp.format('000{}'.format(self.idx)[-4:]), 'w') as f:
            wr = csv.DictWriter(f, fieldnames=self.cols)
            wr.writerows(rows)
        self.idx += 1

    def persistFinal(self):
        with open(self.final, 'w') as f:
            f.write(','.join(self.cols))
            f.write('\n')
            for fn in glob.glob(self.temp.format('*')):
                with open(fn) as part:
                    data = part.read()
                    f.write(data)
                    if not data.endswith('\n'):
                        f.write('\n')
        return pd.read_csv(
            self.final,
            dtype=self.dtypes
        )


class SqlAlchemyPersister(object):
    def __init__(self, connstr, table, extend_existing=False):
        self.connstr = connstr
        self.engine = sqlalchemy.create_engine(self.connstr)
        self.tablename = table
        self.extend_existing = extend_existing
        self.table = None
        self.cols = None
        self.dtypes = None

    def prepare(self, cols, dtypes):
        self.cols = cols
        self.dtypes = dtypes
        with self.engine.begin() as conn:
            md = sqlalchemy.MetaData()
            md.reflect(bind=conn)
            self.table = sqlalchemy.Table(
                self.tablename,
                md,
                *(
                    sqlalchemy.Column(
                        col, sqlalchemy.String
                    )
                    for col in self.cols
                ),
                extend_existing=self.extend_existing
            )
            md.create_all(bind=conn)

    def persistTemp(self, rows):
        with self.engine.begin() as conn:
            conn.execute(self.table.insert(), *list(rows))

    def persistFinal(self):
        ret = pd.read_sql(
            self.table.select(),
            self.engine,
        )
        for col,dtype in self.dtypes.items():
            ret.dtypes[col] = dtype
        return ret

class CensusBulkGeocoder(object):
    """Geocode many addresses."""
    def __init__(
            self,
            persister,
            endpoint='https://geocoding.geo.census.gov/geocoder/geographies/addressbatch',
            benchmark='Public_AR_Current',
            vintage='Current_Current',
            chunksize=1000,
            concurrency=10,
    ):
        self.persister = persister
        self.endpoint = endpoint
        self.benchmark = benchmark
        self.vintage = vintage
        self.chunksize = chunksize
        self.concurrency = concurrency
        # set headers
        self.persister.prepare(
            CENSUS_GEO_COLNAMES,
            CENSUS_GEO_DTYPES,
        )

    def _generate_requests(
            self,
            rows,
            session=None
    ):
        for idx, chunk in enumerate(chunker(self.chunksize, rows)):
            print('Processing chunk #{}: geocoding {} addresses'.format(
                idx,
                len(chunk)))
            sio = StringIO()
            csv.writer(sio).writerows(chunk)
            req = sio.getvalue().rstrip()
            params = {
                'benchmark': self.benchmark,
                'vintage': self.vintage,
            }
            files = {
                'addressFile': ('Addresses.csv', StringIO(req), 'text/csv')
            }
            yield grequests.post(
                self.endpoint,
                params=params,
                files=files,
                stream=False,
                session=session)

    def geocode_rows(self, rows, session=None):
        """Geocode addresses stored as rows.

        Arguments:
          * row: iterator of iterables containing
            * key: unique identifier.
            * street: street address.
            * city: city name.
            * state: state abbreviation.
            * zip5: ZIP code as string.
          * session: requests session to use for calling census API.

        Returns: DataFrame with geocoding output with rows keyed by
          the input key.
        """
        reqiter = self._generate_requests(
            rows,
            session=session)
        reqs = list(reqiter)
        req_to_idx = {req: idx for idx, req in enumerate(reqs)}

        def handleResp(idx, req, retry=True):
            chunkno = req_to_idx.get(req, 'N/A')
            if req.response is not None:
                r = req.response
                print(
                    'Finished req {}/{} for chunk#{}: status {}'.format(
                        idx+1, len(reqs), chunkno, r.status_code))
                if r.status_code == 200:
                    rdr = csv.DictReader(
                        StringIO(r.text),
                        fieldnames=CENSUS_GEO_COLNAMES)
                    self.persister.persistTemp(rdr)
                else:
                    print('Failed req {}/{} for chunk#{}: '
                          'status_code={}'.format(
                              idx+1, len(reqs), chunkno,
                              r.status_code))
                    req.response = None
                    reqs.append(req)
            else:
                print('Failed req {}/{} for chunk#{}: '
                      'exception={}'.format(
                          idx+1, len(reqs), chunkno,
                          req.exception))
                if retry:
                    print('Retrying...')
                    req.send()
                    handleResp(idx, req, False)

        print('Processing {} requests'.format(len(reqs)))
        # reimplement grequests.imap so that we get back the
        # request object to use as a correlator.
        pool = Pool(self.concurrency)
        for idx, req in enumerate(
                pool.imap_unordered(
                    grequests.AsyncRequest.send,
                    reqs)):
            handleResp(idx, req)
        pool.join()
        print('Processed {} responses'.format(idx+1))
        df = self.persister.persistFinal()
        return df

    def geocode_cols(
            self,
            key, street, city, state, zip5,
            session=None
    ):
        """Geocode addresses stored as separate columns.
        Arguments:
          * key: unique identifiers.
          * street: street addresses.
          * city: city names.
          * state: state abbreviations.
          * zip5: ZIP codes as strings.
          * session: requests session to use for calling census API.

        Returns: DataFrame with geocoding output with rows keyed by
          the input key.
        """
        return self.geocode_rows(
            zip(key, street, city, state, zip5),
            session)

    def geocode_df(self, df, columns, session=None):
        """Geocode from a dataframe.

        Arguments:
          * df: the pandas DataFrame
          * columns: a 4- or 5-tuple/list of columns to extract:
            0. unique key. If omitted, index will be used.
            1. street address.
            2. city name.
            3. state abbreviation.
            4. ZIP code as string.
          * session: requests session to use for calling census API.

        Returns: DataFrame with geocoding output with rows keyed by
          the input key.

        Raises:
          ValueError: `columns` has the wrong number of elements.
        """
        if len(columns) == 4:
            it = df.loc[:, columns].itertuples()
        elif len(columns) == 5:
            it = df.loc[:, columns].itertuples(index=False)
        else:
            raise ValueError("len(columns) is neither 4 or 5")
        return self.geocode_rows(
            it,
            session)


def parse_lonlat(series):
    """Turn a Geo.Lon.Lat series into a shapely Geometry series."""
    return series.str.split(',').apply(
        lambda x: x and shapely.geometry.Point(
            float(x[0]), float(x[1])))


def to_geodataframe(df):
    if 'Geo.Lon.Lat' not in df.columns:
        raise ValueError("DataFrame has no Geo.Lon.Lat column.")
    return gpd.GeoDataFrame(df, geometry=parse_lonlat(df['Geo.Lon.Lat']))
