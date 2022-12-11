import logging
from typing import Dict, Optional, Union

import httpx
import pandas as pd

from ..data.model import CensusDataEndpoint
from ..util.errors import CensusError
from ..util.textindex.sqlitefts5index import SqliteFts5Index
from ..util.textindex.textindex import (TextIndex, FieldSet,
                                        DatasetFields)
from ..util.webcache import fetch

_logger = logging.getLogger(__name__)


class DiscoveryInterface:
    """Discover and bind census datasets.

    TODO: Move the functionality into DiscoveryInterface, and make the
    constructor into static method(s).

    """

    datasets: Dict[str, CensusDataEndpoint]
    index: TextIndex
    variableindex: TextIndex

    def __init__(self,
                 key: str,
                 client: httpx.Client,
                 vintage: Optional[Union[str, int]] = None,
                 fts_class: type = SqliteFts5Index):
        """Load and wrap census datasets.

        Prefers cached metadata if present and not stale, otherwise
        queries server.

        Arguments:
          * key: Census API key
          * client: httpx Client to use for calling API.
          * vintage: discovery only data sets for this vintage, if present.
          * fts_class: utility class to use for full-text indices. If omitted,
                SqliteFts5Index will be used.
        """

        self.datasets = {}
        if vintage:
            url = f'https://api.census.gov/data/{vintage}.json'
        else:
            url = 'https://api.census.gov/data.json'
        _logger.debug("Fetching root metadata")
        resp = fetch(url, client).json()
        if not resp:
            raise CensusError("Failed to retrieve root metadata from Census " +
                              "API discovery endpoint")
        datasets = resp.get('dataset')
        if not datasets:
            raise CensusError("Unable to identify datasets from API " +
                              " discovery endpoint")

        _logger.debug("Fetching per-dataset metadata")
        self.index = fts_class(FieldSet.DATASET, 'datasets')
        self.variableindex = fts_class(FieldSet.VARIABLE, 'variables')
        with self.index, self.variableindex:
            for ds in datasets:
                self._process_one_dataset(key, client, ds)

        _logger.debug("Done processing metadata")

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

    def _process_one_dataset(
            self,
            key: str,
            client: httpx.Client,
            ds: dict):
        """Build an CensuDataEndpoint for the specfied dataset
        metadata.

        This must be called in a self.variableindex context.
        """
        ds_id = self._get_ds_id(ds)
        _logger.debug(f'Processing dataset {ds_id}')
        try:
            dataset = CensusDataEndpoint(key, ds, client,
                                         self.variableindex)
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
            _logger.debug('Finished processing metadata for ' +
                          f'dataset: {dataset.id}')
        except Exception as e:
            _logger.warn('Error processing metadata; skipping ' +
                         f'dataset {ds["title"]}', exc_info=e)

    def search(self, query: str):
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

    def __getitem__(self, dataset_id: str):
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
