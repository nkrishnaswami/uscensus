"""Wrappers for the documents in the Census Data API that fetch linked
auxiliary documents on demand.  These delegate most attibute access to
the dataclasses in the `model` module.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, TypeVar

from async_property import async_cached_property

if TYPE_CHECKING:
    import httpx

from uscensus.incremental import model
from uscensus.util.webcache import afetch, fetch

_logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class Geography:
    _model: model.Geography
    levels: dict[str, model.GeographyLevel]
    has_default: bool = False


class Group:
    """A group of related variables on the same topic."""

    def __init__(self, model: model.Group, client: httpx.AsyncClient) -> None:
        self._model = model
        self.client = client

    def __getattr__(self, attr: str):
        return getattr(self._model, attr)

    @cached_property
    def variables(self) -> dict[str, model.Variable]:
        if self._model.variables:
            url = self._model.variables.replace('http:', 'https:')
            return model.Variables.from_json(
                fetch(url, self.client).text).variables
        return {}

    @async_cached_property
    async def avariables(self) -> dict[str, model.Variable]:
        if self._model.variables:
            url = self._model.variables.replace('http:', 'https:')
            return model.Variables.from_json(
                ((await afetch(url, self.client))).text).variables
        return {}


class Dataset:
    def __init__(self, model: model.Dataset, client: httpx.AsyncClient) -> None:
        self._model = model
        self.client = client

    def __repr__(self) -> str:
        return f'<{self._model.title}:{self._model.c_vintage}:{self._model.c_dataset}>'

    def __getattr__(self, attr: str):
        """Delegate attribute access to model.Catalog."""
        return getattr(self._model, attr)

    @cached_property
    def geography(self) -> Geography:
        """Retrieve and process the geography link, if any.

        Returns
        -------
          The wrapped model instance.
        """
        if self._model.c_geographyLink:
            url = self._model.c_geographyLink.replace('http:', 'https:')
            _logger.debug('Fetching geographies: %s', url)
            geography = model.Geography.from_json(fetch(url, self.client).text)
            return Geography(geography,
                             {level.name: level for level in geography.fips},
                             has_default=bool(geography.default and
                                              any(x.isDefault == 'true' for x in geography.default)))
        return Geography(_model=model.Geography([], []),
                         levels={},
                         has_default=False)

    @async_cached_property
    async def ageography(self) -> Geography:
        """Retrieve and process the geography link, if any.

        Returns
        -------
          The wrapped model instance.
        """
        if self._model.c_geographyLink:
            url = self._model.c_geographyLink.replace('http:', 'https:')
            _logger.debug('Fetching geographies: %s', url)
            geography = model.Geography.from_json((await afetch(url, self.client)).text)
            return Geography(geography,
                             {level.name: level for level in geography.fips},
                             has_default=bool(geography.default and
                                              geography.default[0].isDefault == 'true'))
        return Geography(_model=model.Geography([], []),
                         levels={},
                         has_default=False)

    @cached_property
    def tags(self) -> list[str]:
        """Retrieve and process the tags link, if any.

        Returns
        -------
          The tag values as a list.
        """
        if self._model.c_tagsLink:
            url = self._model.c_tagsLink.replace('http:', 'https:')
            _logger.debug('Fetching tags:  %s', url)
            return model.Tags.from_json(fetch(url, self.client).text).tags
        return []

    @async_cached_property
    async def atags(self) -> list[str]:
        """Retrieve and process the tags link, if any.

        Returns
        -------
          The tag values as a list.
        """
        if self._model.c_tagsLink:
            url = self._model.c_tagsLink.replace('http:', 'https:')
            _logger.debug('Fetching tags:  %s', url)
            return model.Tags.from_json((await afetch(url, self.client)).text).tags
        return []

    @cached_property
    def groups(self) -> dict[str, Group]:
        """Retrieve and process the variable groups link, if any.

        Returns
        -------
          Wrappers for each variable group as a dict keyed by group ID.
        """
        if self._model.c_groupsLink:
            url = self._model.c_groupsLink.replace('http:', 'https:')
            _logger.debug('Fetching groups:  %s', url)
            # The field name "universe" has a trailing space in the census data.
            return {
                group_dict['name']:
                Group(model.Group.from_dict({key.strip(): value
                                             for key, value in group_dict.items()}),
                      self.client)
                for group_dict in fetch(url, self.client).json()['groups']
            }
        return {}

    @async_cached_property
    async def agroups(self) -> dict[str, Group]:
        """Retrieve and process the variable groups link, if any.

        Returns
        -------
          Wrappers for each variable group as a dict keyed by group ID.
        """
        if self._model.c_groupsLink:
            url = self._model.c_groupsLink.replace('http:', 'https:')
            _logger.debug('Fetching groups:  %s', url)
            return {
                group_dict['name']:
                Group(model.Group.from_dict({key.strip(): value
                                             for key, value in group_dict.items()}),
                      self.client)
                for group_dict in ((await afetch(url, self.client)).json())['groups']
            }
        return {}

    @cached_property
    def variables(self) -> dict[str, model.Variable]:
        """Retrieve and process the variables link, if any.

        Returns
        -------
          The variables in a dict keyed by ID.
        """
        if self._model.c_variablesLink:
            url = self._model.c_variablesLink.replace('http:', 'https:')
            _logger.debug('Fetching variables:  %s', url)
            return model.Variables.from_json(
                fetch(url, self.client).text).variables
        return {}

    @async_cached_property
    async def avariables(self) -> dict[str, model.Variable]:
        """Retrieve and process the variables link, if any.

        Returns
        -------
          The variables in a dict keyed by ID.
        """
        if self._model.c_variablesLink:
            url = self._model.c_variablesLink.replace('http:', 'https:')
            _logger.debug('Fetching variables:  %s', url)
            return model.Variables.from_json(
                (await afetch(url, self.client)).text).variables
        return {}

    @cached_property
    def api_url(self) -> str:
        """Find the distribution link for the JSON API, if any.

        Returns
        -------
          The URL if present, otherwise the empty string.
        """
        for distribution in self._model.distribution:
            if (distribution.format == 'API' and
                    distribution.mediaType == 'application/json'):
                return distribution.accessURL.replace('http:', 'https:')
        return ''

    @cached_property
    def weight_variables(self) -> dict[str, model.Variable] | None:
        if not self._model.c_isMicrodata:
            return None
        ret = {}
        for name, variable in self.variables.items():
            if variable.isWeight:
                ret[name] = variable
        return ret

    @cached_property
    def required_variables(self) -> dict[str, model.Variable] | None:
        ret = {}
        for name, variable in self.variables.items():
            if variable.required:
                ret[name] = variable
        return ret

    @async_cached_property
    async def weight_avariables(self) -> dict[str, model.Variable] | None:
        if not self._model.c_isMicrodata:
            return None
        variables = await self.avariables
        ret = {}
        for name, variable in variables.items():
            if variable.isWeight:
                ret[name] = variable
        return ret

    @async_cached_property
    async def required_avariables(self) -> dict[str, model.Variable] | None:
        variables = await self.avariables
        ret = {}
        for name, variable in variables.items():
            if variable.required:
                ret[name] = variable
        return ret


CAT = TypeVar('CAT', bound='Catalog')


class Catalog:
    @classmethod
    def get_catalog(cls: type[CAT],
                    client: httpx.AsyncClient,
                    *,
                    catalog_subpath: str = '') -> Catalog:
        """Retrieve and process the root or subpath data catalog
        document from the Census API server.

        Arguments:
        ---------
          * client: an httpx.AsyncClient instance, such as one
                returned by uscensus.util.webcache.make_client.

        Returns:
        -------
          The new Catalog wrapper instance.
        """
        if not catalog_subpath:
            url = 'https://api.census.gov/data.json'
        else:
            url = f'https://api.census.gov/data/{catalog_subpath}.json'
        _logger.debug('Fetching catalog:  %s', url)
        return cls(model.Catalog.from_json(fetch(url, client).text), client)

    @classmethod
    async def aget_catalog(cls: type[CAT],
                           client: httpx.AsyncClient,
                           *,
                           catalog_subpath: str = '') -> Catalog:
        """Retrieve and process the root or subpath data catalog
        document from the Census API server.

        Arguments:
        ---------
          * client: an httpx.AsyncClient instance, such as one
                returned by uscensus.util.webcache.make_client.

        Returns:
        -------
          The new Catalog wrapper instance.
        """
        if not catalog_subpath:
            url = 'https://api.census.gov/data.json'
        else:
            url = f'https://api.census.gov/data/{catalog_subpath}.json'
        _logger.debug('Fetching catalog:  %s', url)
        return cls(model.Catalog.from_json((await afetch(url, client)).text), client)

    def __init__(self, model: model.Catalog, client: httpx.AsyncClient) -> None:
        self._model = model
        self.client = client

    def __getattr__(self, attr: str):
        """Delegate attribute access to model.Catalog."""
        return getattr(self._model, attr)

    @cached_property
    def dataset(self) -> list[Dataset]:
        """Return wrapped Dataset instances for each dataset in the
        catalog.
        """
        return [Dataset(dataset, self.client)
                for dataset in self._model.dataset]
