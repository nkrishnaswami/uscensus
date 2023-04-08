from arcgis.gis.server.catalog import ServicesDirectory

_TIGERWEB_CATALOG = 'https://tigerweb.geo.census.gov/arcgis/rest/services'


def get_tigerweb_catalog():
    return ServicesDirectory(_TIGERWEB_CATALOG, all_ssl=True, initialize=True)
