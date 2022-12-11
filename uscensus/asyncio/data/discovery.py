import logging
from typing import Dict, List, Optional, Union

import aiotools
import httpx
import pandas as pd

from ...util.errors import CensusError
from ...util.textindex import TextIndex, FieldSet, DatasetFields
from ...util.textindex.sqlitefts5index import SqliteFts5Index
from ...asyncio.data.model import AsyncCensusDataEndpoint
from ...asyncio.util.webcache import afetch

_logger = logging.getLogger(__name__)


class AsyncDiscoveryInterface:
    """Discover and bind census datasets.

    TODO: Move the functionality into DiscoveryInterface, and make the
    constructor into static method(s).

    """

    datasets: Dict[str, AsyncCensusDataEndpoint]
    index: TextIndex
    variableindex: TextIndex

    @staticmethod
    async def create(key: str,
                     client: httpx.AsyncClient,
                     vintage: Optional[Union[str, int]] = None,
                     fts_class: type = SqliteFts5Index):
        """Load and wrap census datasets.

        Prefers cached metadata if present and not stale, otherwise
        queries server.

        Arguments:
          * key: Census dataset key
          * client: httpx Client to use for accessing API.
          * vintage: discovery only data sets for this vintage, if present.
          * fts_class: utility class to use for full-text indices. If omitted,
                SqliteFts5Index will be used.
        """
        self = AsyncDiscoveryInterface()

        self.datasets = {}
        if vintage:
            url = f'https://api.census.gov/data/{vintage}.json'
        else:
            url = 'https://api.census.gov/data.json'
        _logger.debug("Fetching root metadata")
        r = await afetch(url, client)
        resp = r.json()
        datasets = resp.get('dataset')
        if not datasets:
            raise CensusError("Unable to identify datasets from dataset " +
                              " discovery endpoint")

        self.index = fts_class(FieldSet.DATASET, 'datasets')
        self.variableindex = fts_class(FieldSet.VARIABLE, 'variables')
        with self.index, self.variableindex:
            async with aiotools.TaskGroup(name='discovery') as tg:
                complete = [0]
                for ds in datasets:
                    tg.create_task(
                        self._process_one_dataset(key, client, ds, complete),
                        name=ds['title'])

        _logger.info("Done processing datasets")
        return self

    @staticmethod
    def _get_ds_id(ds: dict):
        for distribution in ds.get('distribution') or []:
            if distribution.get('format') == 'API':
                endpoint = distribution['accessURL']
        return endpoint.replace(
            'http://api.census.gov/data/', ''
        ).replace(
            'https://api.census.gov/data/', ''
        )

    async def _process_one_dataset(self,
                                   key: str,
                                   client: httpx.AsyncClient,
                                   ds: dict,
                                   complete: List[int]):
        """Build an AsyncCensuDataEndpoint for the specfied dataset
        metadata.

        This must be called with self.index and self.variableindex
        entered.

        """
        ds_id = self._get_ds_id(ds)
        _logger.debug(f'Processing dataset {ds_id}')
        try:
            dataset = await AsyncCensusDataEndpoint.create(
                key, ds, client, self.variableindex)
            # TODO: add more indexing; groups, hier by
            #       dataset, geo schemes, by vintage, etc
            self.datasets[dataset.id] = dataset
            self.index.add([
                DatasetFields(
                    dataset_id=dataset.id,
                    title=dataset.title,
                    description=dataset.description,
                    geographies=' '.join(dataset.geographies[
                        'name'].astype(str)),
                    concepts=' '.join(dataset.concepts),
                    keywords=' '.join(dataset.keywords),
                    tags=' '.join(dataset.tags),
                    variables=' '.join(dataset.variables['label']),
                    vintage=dataset.vintage)])
            _logger.debug('Finished processing metadata for dataset: ' +
                          f'{dataset.id}')
        except Exception as e:
            _logger.warn('Error processing metadata; skipping dataset ' +
                         f'{ds["title"]}: {ds_id}', exc_info=e)
            return
        complete[0] += 1
        if complete[0] % 100 == 0:
            _logger.info(f'Processed {complete[0]} datasets')


    def search(self, query):
        """Find a list of dataset objects matching the index query.
        Index queries default to searching dataset titles, but may also
        search

            * description: long description of an dataset
            * variables: variables to return from query
            * geographies: either variables to return from or to
              constrain a query
            * concepts: groupings of variables
            * keywords
            * tags

        by prefixing an individual term or groups of terms in parentheses
        by the field name and a colon.

        Elaborate queries can be constructed using parenthesized
        subqueries, ANDs, and ORs.
        """
        if query.find(':') < 0:
            query = 'title: ' + query

        cols = ['score', 'dataset_id', 'title', 'description']
        return pd.DataFrame(
            [tuple((row[col] for col in cols)) for row in self.index.query(query)],
            columns=cols
        )

    def __getitem__(self, dataset_id):
        """Return an identifier by dataset ID.

        Arguments:
          * dataset_id: the part of its endpoint name without the shared
                census dataset URL prefix.
        """

        return self.datasets.get(dataset_id)

    def __repr__(self):
        """The readable string for an Loader is that of its `datasets`
        dictionary.
        """

        return repr(self.datasets)
