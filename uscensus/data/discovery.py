import logging
from typing import Optional, Union

import httpx
import pandas as pd

from ..data.model import CensusDataEndpoint
from ..util.errors import CensusError
from ..util.webcache import fetch
from ..util.textindex.textindex import (TextIndex, FieldSet,
                                        DatasetFields)

_logger = logging.getLogger(__name__)


class DiscoveryInterface(object):
    """Discover and bind census datasets.

    TODO: Move the functionality into DiscoveryInterface, and make the
    constructor into static method(s).

    """

    index: TextIndex
    variableindex: TextIndex

    def __init__(self,
                 key: str,
                 session: httpx.Client,
                 vintage: Optional[Union[str, int]] = None,
                 fts_class: Optional[type] = None):
        """Load and wrap census datasets.

        Prefers cached metadata if present and not stale, otherwise
        queries server.

        Arguments:
          * key: Census API key
          * session: httpx Client to use for calling API.
          * vintage: discovery only data sets for this vintage, if present.
          * fts_class: utility class to use for full-text indices. If omitted,
                SqliteFts5Index will be used.
        """

        if fts_class is None:
            from ..util.textindex.sqlitefts5index import SqliteFts5Index
            fts_class = SqliteFts5Index

        self.datasets = {}
        if vintage:
            url = f'https://api.census.gov/data/{vintage}.json'
        else:
            url = 'https://api.census.gov/data.json'
        _logger.debug("Fetching root metadata")
        resp = fetch(url, session).json()
        if not resp:
            raise CensusError("Failed to retrieve root metadata from Census " +
                              "API discovery endpoint")
        datasets = resp.get('dataset')
        if not datasets:
            raise CensusError("Unable to identify datasets from API " +
                              " discovery endpoint")

        _logger.debug("Fetching per-dataset metadata")
        self.variableindex = fts_class(FieldSet.VARIABLE, 'variables')
        with self.variableindex:
            for ds in datasets:
                for distribution in ds.get('distribution') or []:
                    if distribution.get('format') == 'API':
                        endpoint = distribution['accessURL']
                ds_id = endpoint.replace(
                    'http://api.census.gov/data/', ''
                ).replace(
                    'https://api.census.gov/data/', ''
                )
                _logger.debug(f'Processing dataset {ds_id}')
                try:
                    dataset = CensusDataEndpoint(key, ds, session,
                                             self.variableindex)
                    # TODO: add more indexing; groups, hier by
                    #       dataset, geo schemes, by vintage, etc
                    self.datasets[dataset.id] = dataset
                    _logger.debug('Finished processing metadata for dataset: ' +
                                  f'{dataset.id}')
                except Exception as e:
                    _logger.warn('Error processing metadata; skipping dataset ' +
                                 f'{ds["title"]}', exc_info=e)

        _logger.debug("Indexing dataset metadata")
        self.index = fts_class(FieldSet.DATASET, 'datasets')
        with self.index:
            self.index.add(
                DatasetFields(
                    dataset_id=dataset.id,
                    title=dataset.title,
                    description=dataset.description,
                    geographies=' '.join(dataset.geographies['name']),
                    concepts=' '.join(dataset.concepts),
                    keywords=' '.join(dataset.keywords),
                    tags=' '.join(dataset.tags),
                    variables=' '.join(dataset.variables['label']),
                    vintage=dataset.vintage)
                for dataset in self.datasets.values()
            )
            _logger.debug("Done adding metadata")
        _logger.debug("Done committing metadata")

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
