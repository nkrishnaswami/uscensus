"""Wrappers for the documents in the Census Data API that fetch linked
auxiliary documents on demand.  These delegate most attibute access to
the dataclasses in the `model` module.
"""
from __future__ import annotations

import json
import logging
from functools import cached_property
from typing import Callable, TypeVar, cast

import httpx
from async_property import async_cached_property

from uscensus.incremental import model
from uscensus.util.webcache import afetch, fetch

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

class _ModelDelegate:
    """Mixin providing attribute delegation to `self._model`.

    Any attribute not found on the wrapper class itself (including
    cached_property/async_cached_property descriptors) falls through to
    the wrapped pydantic model instance.
    """

    _model: object

    def __getattr__(self, attr: str):
        cls = type(self)
        descriptor = getattr(cls, attr, None)
        if descriptor is not None and hasattr(descriptor, '__get__'):
            return descriptor.__get__(self, cls)
        return getattr(self._model, attr)


async def _fetch_bytes(url: str, client: httpx.AsyncClient | httpx.Client) -> bytes:
    """Fetch a URL's body, dispatching to sync or async I/O based on the
    client type. When `client` is a plain httpx.Client this never
    actually suspends, so it's safe to call from a coroutine driven
    purely for its side of a synchronous cached_property.
    """
    if isinstance(client, httpx.AsyncClient):
        return (await afetch(url, client)).content
    return fetch(url, cast(httpx.Client, client)).content


def _filter_variables(
    variables: dict[str, model.Variable],
    predicate: Callable[[model.Variable], bool],
) -> dict[str, model.Variable]:
    return {name: v for name, v in variables.items() if predicate(v)}


class Geography(model.USCensusBaseModel):
    _model: model.Geography
    levels: dict[str, list[model.GeographyLevel]]
    has_default: bool = False


_EMPTY_GEOGRAPHY = Geography(
    _model=model.Geography(fips=[], default=[]),
    levels={},
    has_default=False,
)


def _parse_geography(content: bytes) -> Geography:
    geography = model.Geography.model_validate_json(content)
    levels: dict[str, list[model.GeographyLevel]] = {}
    for level in geography.fips:
        levels.setdefault(level.name, []).append(level)
    # NOTE: the previous sync/async implementations disagreed here
    # (any(...) vs. only checking default[0]); any(...) is the correct
    # semantics -- a dataset can list multiple default flags.
    has_default = bool(
        geography.default and any(x.isDefault == 'true' for x in geography.default)
    )
    return Geography(_model=geography, levels=levels, has_default=has_default)


# ---------------------------------------------------------------------------
# Group
# ---------------------------------------------------------------------------

class Group(_ModelDelegate):
    """A group of related variables on the same topic."""

    _model: model.Group

    def __init__(self, model: model.Group, client: httpx.AsyncClient | httpx.Client) -> None:
        self._model = model
        self.client = client

    @cached_property
    def variables(self) -> dict[str, model.Variable]:
        if not self._model.variables:
            return {}
        url = self._model.variables.replace('http:', 'https:')
        content = fetch(url, cast(httpx.Client, self.client)).content
        return model.Variables.model_validate_json(content).variables

    @async_cached_property
    async def avariables(self) -> dict[str, model.Variable]:
        if not self._model.variables:
            return {}
        url = self._model.variables.replace('http:', 'https:')
        content = await _fetch_bytes(url, self.client)
        return model.Variables.model_validate_json(content).variables


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class Dataset(_ModelDelegate):
    _model: model.Dataset

    def __init__(self, model: model.Dataset, client: httpx.AsyncClient | httpx.Client) -> None:
        self._model = model
        self.client = client

    def __repr__(self) -> str:
        return f'<{self._model.title}:{self._model.c_vintage}:{self._model.c_dataset}>'

    # -- geography -----------------------------------------------------

    @cached_property
    def geography(self) -> Geography:
        """Retrieve and process the geography link, if any."""
        if not self._model.c_geographyLink:
            return _EMPTY_GEOGRAPHY
        url = self._model.c_geographyLink.replace('http:', 'https:')
        _logger.debug('Fetching geographies: %s', url)
        content = fetch(url, cast(httpx.Client, self.client)).content
        return _parse_geography(content)

    @async_cached_property
    async def ageography(self) -> Geography:
        """Retrieve and process the geography link, if any."""
        if not self._model.c_geographyLink:
            return _EMPTY_GEOGRAPHY
        url = self._model.c_geographyLink.replace('http:', 'https:')
        _logger.debug('Fetching geographies: %s', url)
        content = await _fetch_bytes(url, self.client)
        return _parse_geography(content)

    # -- tags ------------------------------------------------------------

    @cached_property
    def tags(self) -> list[str]:
        """Retrieve and process the tags link, if any."""
        if not self._model.c_tagsLink:
            return []
        url = self._model.c_tagsLink.replace('http:', 'https:')
        _logger.debug('Fetching tags: %s', url)
        content = fetch(url, cast(httpx.Client, self.client)).content
        return model.Tags.model_validate_json(content).tags

    @async_cached_property
    async def atags(self) -> list[str]:
        """Retrieve and process the tags link, if any."""
        if not self._model.c_tagsLink:
            return []
        url = self._model.c_tagsLink.replace('http:', 'https:')
        _logger.debug('Fetching tags: %s', url)
        content = await _fetch_bytes(url, self.client)
        return model.Tags.model_validate_json(content).tags

    # -- groups ------------------------------------------------------------

    @staticmethod
    def _parse_groups(content: bytes, client: httpx.AsyncClient | httpx.Client) -> dict[str, Group]:
        # The field name "universe" has a trailing space in the census data.
        return {
            group_dict['name']: Group(
                model.Group.model_validate(
                    {key.strip(): value for key, value in group_dict.items()}
                ),
                client,
            )
            for group_dict in json.loads(content)['groups']
        }

    @cached_property
    def groups(self) -> dict[str, Group]:
        """Retrieve and process the variable groups link, if any."""
        if not self._model.c_groupsLink:
            return {}
        url = self._model.c_groupsLink.replace('http:', 'https:')
        _logger.debug('Fetching groups: %s', url)
        content = fetch(url, cast(httpx.Client, self.client)).content
        return self._parse_groups(content, self.client)

    @async_cached_property
    async def agroups(self) -> dict[str, Group]:
        """Retrieve and process the variable groups link, if any."""
        if not self._model.c_groupsLink:
            return {}
        url = self._model.c_groupsLink.replace('http:', 'https:')
        _logger.debug('Fetching groups: %s', url)
        content = await _fetch_bytes(url, self.client)
        return self._parse_groups(content, self.client)

    # -- variables -----------------------------------------------------

    @cached_property
    def variables(self) -> dict[str, model.Variable]:
        """Retrieve and process the variables link, if any."""
        if not self._model.c_variablesLink:
            return {}
        url = self._model.c_variablesLink.replace('http:', 'https:')
        _logger.debug('Fetching variables: %s', url)
        content = fetch(url, cast(httpx.Client, self.client)).content
        return model.Variables.model_validate_json(content).variables

    @async_cached_property
    async def avariables(self) -> dict[str, model.Variable]:
        """Retrieve and process the variables link, if any."""
        if not self._model.c_variablesLink:
            return {}
        url = self._model.c_variablesLink.replace('http:', 'https:')
        _logger.debug('Fetching variables: %s', url)
        content = await _fetch_bytes(url, self.client)
        return model.Variables.model_validate_json(content).variables

    # -- derived, non-fetching properties -------------------------------

    @cached_property
    def api_url(self) -> str:
        """Find the distribution link for the JSON API, if any."""
        for distribution in self._model.distribution:
            if (distribution.format == 'API' and
                    distribution.mediaType == 'application/json'):
                return distribution.accessURL.replace('http:', 'https:')
        return ''

    @cached_property
    def weight_variables(self) -> dict[str, model.Variable] | None:
        if not self._model.c_isMicrodata:
            return None
        return _filter_variables(self.variables, lambda v: bool(v.isWeight))

    @cached_property
    def required_variables(self) -> dict[str, model.Variable]:
        return _filter_variables(self.variables, lambda v: bool(v.required))

    @async_cached_property
    async def weight_avariables(self) -> dict[str, model.Variable] | None:
        if not self._model.c_isMicrodata:
            return None
        return _filter_variables(await self.avariables, lambda v: bool(v.isWeight))

    @async_cached_property
    async def required_avariables(self) -> dict[str, model.Variable]:
        return _filter_variables(await self.avariables, lambda v: bool(v.required))


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

CAT = TypeVar('CAT', bound='Catalog')


class Catalog(_ModelDelegate):
    _model: model.Catalog

    @staticmethod
    def _catalog_url(catalog_subpath: str) -> str:
        if not catalog_subpath:
            return 'https://api.census.gov/data.json'
        return f'https://api.census.gov/data/{catalog_subpath}.json'

    @classmethod
    def get_catalog(
        cls: type[CAT],
        client: httpx.Client,
        *,
        catalog_subpath: str = '',
    ) -> Catalog:
        """Retrieve and process the root or subpath data catalog
        document from the Census API server.
        """
        url = cls._catalog_url(catalog_subpath)
        _logger.debug('Fetching catalog: %s', url)
        content = fetch(url, client).content
        return cls(model=model.Catalog.model_validate_json(content), client=client)

    @classmethod
    async def aget_catalog(
        cls: type[CAT],
        client: httpx.AsyncClient,
        *,
        catalog_subpath: str = '',
    ) -> Catalog:
        """Retrieve and process the root or subpath data catalog
        document from the Census API server.
        """
        url = cls._catalog_url(catalog_subpath)
        _logger.debug('Fetching catalog: %s', url)
        content = (await afetch(url, client)).content
        return cls(model=model.Catalog.model_validate_json(content), client=client)

    def __init__(self, model: model.Catalog, client: httpx.AsyncClient | httpx.Client) -> None:
        self._model = model
        self.client = client

    @cached_property
    def dataset(self) -> list[Dataset]:
        """Return wrapped Dataset instances for each dataset in the
        catalog.
        """
        return [Dataset(dataset, self.client) for dataset in self._model.dataset]
