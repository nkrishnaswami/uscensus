from __future__ import print_function
from __future__ import unicode_literals

from ..util.errors import CensusError
from ..util.nopcache import NopCache
from ..data.discovery import DiscoveryInterface

import json
from datetime import datetime as dt
try:
    from email.utils import format_datetime
except ImportError:
    # The fn was introduced in 3.3; hack:
    from email.utils import formatdate
    import time

    def format_datetime(dt):
        now = time.mktime(dt.timetuple())
        return formatdate(now)


class FakeResponse(object):
    def __init__(self):
        self.text = ''
        self.status_code = 200
        self.headers = {
            'Date': format_datetime(dt.now())
        }

    def raise_for_status(self):
        pass


class FakeSession(object):
    def get(self, url, **kwargs):
        ret = FakeResponse()
        if url.endswith('data.json'):
            ret.text = self.getData()
        elif url.endswith('variables.json'):
            ret.text = self.getVars()
        elif url.endswith('geography.json'):
            ret.text = self.getGeos()
        elif url.endswith('tags.json'):
            ret.text = self.getTags()
        else:
            print('Unexpected url:', url)
        return ret

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


def DiscoveryInterface_test():
    cl = DiscoveryInterface('', NopCache(), FakeSession())
    print(cl.apis)
    assert len(cl.apis) == 1
    k, v = next(iter(cl.apis.items()))
    print("key is", k)
    assert k == 'timeseries/poverty/histpov2'
    assert v.tags == ['poverty']
