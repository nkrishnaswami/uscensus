import json
import logging

import httpx
from httpx_caching import CachingClient
import pytest

from .discovery import AsyncDiscoveryInterface


_logger = logging.getLogger(__name__)


class FakeAsyncTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, req):
        if req.url.path.endswith('data.json'):
            content = self.getData()
        elif req.url.path.endswith('variables.json'):
            content = self.getVars()
        elif req.url.path.endswith('geography.json'):
            content = self.getGeos()
        elif req.url.path.endswith('tags.json'):
            content = self.getTags()
        else:
            _logger.warn('Unexpected url:', req.url)
        return httpx.Response(200,
                              headers={'content-type': 'application/json'},
                              content=content)

    # Yeah, it's gauche to mix quote types, but these are basically
    # the repr's for some returned objects, and many of the strings
    # contain single quotes; seems cleaner than escaping them all.
    def getData(self):
        return json.dumps({
            "dataset": [
                {
                    "title": "Time Series Current Population Survey: Poverty Status",
                    "description": "The Current Population Survey (CPS), sponsored jointly by the U.S. Census Bureau and the U.S. Bureau of Labor Statistics (BLS), is the primary source of labor force statistics for the population of the United States. The CPS is the source of numerous high-profile economic statistics, including the national unemployment rate, and provides data on a wide range of issues relating to employment and earnings. The CPS also collects extensive demographic data that complement and enhance our understanding of labor market conditions in the nation overall, among many different population groups, in the states and in substate areas.",
                    "c_dataset": [
                        "timeseries",
                        "poverty",
                        "histpov2"
                    ],
                    "c_vintage": "2015",
                    "distribution": [
                        {
                            "accessURL": "https://api.census.gov/data/timeseries/poverty/histpov2",
                            "format": "API"
                        }
                    ],
                    "c_geographyLink": "https://api.census.gov/data/timeseries/poverty/histpov2/geography.json",
                    "c_variablesLink": "https://api.census.gov/data/timeseries/poverty/histpov2/variables.json",
                    "c_tagsLink": "https://api.census.gov/data/timeseries/poverty/histpov2/tags.json"
                }
            ]
        })

    def getVars(self):
        return json.dumps({
            "variables": {
                "for": {
                    "label": "Census API FIPS 'for' clause",
                    "concept": "Census API Geography Specification",
                    "predicateType": "fips-for",
                    "predicateOnly": True
                },
                "in": {
                    "label": "Census API FIPS 'in' clause",
                    "concept": "Census API Geography Specification",
                    "predicateType": "fips-in",
                    "predicateOnly": True
                },
                "time": {
                    "label": "ISO-8601 Date/Time value",
                    "concept": "Census API Date/Time Specification",
                    "required": True,
                    "predicateType": "datetime",
                    "predicateOnly": True,
                    "datetime": {
                        "year": True
                    }
                },
                "FEMHHPOV": {
                    "label": "People in Families!!Female HH no HB!!Below Poverty Level!!Number  NOTE: Numbers in thousands. People as of March of the following year",
                    "concept": "Poverty Statistics",
                    "predicateType": "int"
                },
                "FOOTID": {
                    "label": "Footnotes found at https://www.census.gov/hhes/www/poverty/histpov/footnotes.html",
                    "concept": "Poverty Statistics",
                    "predicateType": "int"
                }
            }
        })

    def getGeos(self):
        return json.dumps({
            "fips": [
                {
                    "name": "us",
                    "geoLevelId": "010",
                    "default": [
                        {
                            "isDefault": "true"
                        }
                    ]
                }
            ]
        })

    def getTags(self):
        return json.dumps({
            "tags": [
                "poverty"
            ]
        })


@pytest.mark.asyncio
async def test_AsyncDiscoveryInterface():

    cl = await AsyncDiscoveryInterface.create(
        '', CachingClient(httpx.AsyncClient(
            transport=FakeAsyncTransport())))
    _logger.info(f'APIs are {cl.apis}')
    assert len(cl.apis) == 1
    k, v = next(iter(cl.apis.items()))
    _logger.info(f'first key is {k}')
    assert k == 'timeseries/poverty/histpov2'
    assert v.tags == ['poverty']
