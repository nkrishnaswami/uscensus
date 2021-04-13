"""Bulk geocoding API wrapper.

Attributes:
    CENSUS_GEO_COLNAMES: column names in output from geocoding API.
"""
import asyncio
import csv
from io import StringIO
from itertools import islice
import logging
import glob
import os
import os.path

import geopandas as gpd
import httpx
import pandas as pd
import shapely
import sqlalchemy


_logger = logging.getLogger(__name__)

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
        with open(self.temp.format(f'{self.idx:04}'), 'w') as f:
            wr = csv.DictWriter(f, fieldnames=self.cols)
            wr.writerows(rows)
        self.idx += 1

    def persistFinal(self):
        with open(self.final, 'w') as f:
            f.write(','.join(self.cols))
            f.write('\n')
            for fn in sorted(glob.glob(self.temp.format('*'))):
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
        for col, dtype in self.dtypes.items():
            ret.dtypes[col] = dtype
        return ret


class CensusBulkGeocoder(object):
    """Geocode many addresses."""

    BATCH_ENDPOINT = ('https://geocoding.geo.census.gov/geocoder/' +
                      'geographies/addressbatch')

    def __init__(
            self,
            persister,
            endpoint=BATCH_ENDPOINT,
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
            client
    ):
        for idx, chunk in enumerate(chunker(self.chunksize, rows)):
            _logger.debug(f'Processing chunk #{idx}: geocoding {len(chunk)} ' +
                          'addresses')
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
            yield client.build_request(
                'POST',
                self.endpoint,
                params=params,
                files=files)

    async def _async_geocode_rows(self, rows):
        async with httpx.AsyncClient(
                limits=httpx.Limits(max_connections=self.concurrency)
        ) as client:
            reqiter = self._generate_requests(rows, client)
            reqs = list(reqiter)
            req_to_chunkno = {req: chunkno for chunkno, req in enumerate(reqs)}

            async def handleResp(idx, r, retry=True):
                chunkno = req_to_chunkno.get(r.request, 'N/A')
                _logger.debug(f'Finished req {idx+1}/{len(reqs)} for ' +
                              f'chunk {chunkno}: status {r.status_code}')
                if r.status_code == 200:
                    rdr = csv.DictReader(
                        StringIO(r.text),
                        fieldnames=CENSUS_GEO_COLNAMES)
                    self.persister.persistTemp(rdr)
                else:
                    _logger.warn(f'Failed req {idx+1}/{len(reqs)} for ' +
                                 f'chunk {chunkno}: ' +
                                 f'status_code={r.status_code}')
                    if retry:
                        _logger.info('Retrying...')
                        handleResp(idx, await client.send(r.request), False)

            _logger.debug(f'Processing {len(reqs)} requests')
            for idx, resp in enumerate(asyncio.as_completed(
                    [client.send(req) for req in reqs])):
                await handleResp(idx, await resp)
        _logger.debug(f'Processed {idx+1} responses')

    def geocode_rows(self, rows):
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
        asyncio.run(self._async_geocode_rows(rows))
        df = self.persister.persistFinal()
        return df

    def geocode_cols(
            self,
            key, street, city, state, zip5,
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
            zip(key, street, city, state, zip5))

    def geocode_df(self, df, columns):
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
            iter = df.loc[:, columns].itertuples()
        elif len(columns) == 5:
            iter = df.loc[:, columns].itertuples(index=False)
        else:
            raise ValueError("len(columns) is neither 4 or 5")
        return self.geocode_rows(iter)


def parse_lonlat(series):
    """Turn a Geo.Lon.Lat series into a shapely Geometry series."""
    return series.str.split(',').apply(
        lambda x: x and shapely.geometry.Point(
            float(x[0]), float(x[1])))


def to_geodataframe(df):
    if 'Geo.Lon.Lat' not in df.columns:
        raise ValueError("DataFrame has no Geo.Lon.Lat column.")
    return gpd.GeoDataFrame(df, geometry=parse_lonlat(df['Geo.Lon.Lat']))
