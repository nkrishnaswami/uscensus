# Census API helpers
[![Build Status](https://travis-ci.org/nkrishnaswami/uscensus.svg?branch=master)](https://travis-ci.org/nkrishnaswami/uscensus)

This module fetches metadata for the Census Data API to make it easier to find relevant data sets and variables to query, and returns data as a Pandas dataframe.

See the example notebook for basic usage.

# Installation
The package is available on PyPi, so you can install the latest version using `pip`:
```bash
pip install uscensus
```

## API Key
You will need an API key to invoke the census APIs.  You can request one here:  
  http://api.census.gov/data/key_signup.html

In the example notebook, I read an API key from a config file, viz. `~/.census` using `RawConfigParser`.

# Census APIs
The official Census API documention is at
  https://www.census.gov/developers/

## Census Data APIs
The Census Data APIs are the first set of interfaces to which this package provides access. These cover the decennial census, American Community Survey, and other data sets for a variety of vintages.

This package uses the (Census Data API Discovery Interface)[https://api.census.gov/data.json] to identify the currently available
data sets, and caches the data in a database (eg `sqlite3` db).  The
discovery interface is an instance of the (US Open Project Data Common
Core Metadata Schema)[https://project-open-data.cio.gov/schema/].

For detailed documentation, see the (Census Data API User Guide [pdf])[https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf]

## Geocoder REST services
The Census Bureau offers REST Services for geocoding addresses to
canonical form, lon/lat, and Census geographies, based on the Master
Address File and TIGER data. These support single address queries as
well as a bulk interface for up to 1000 addresses at a time.

This package provides pandas and raw interfaces to both query styles. [Note: not pushed yet, writing test cases.]

## TigerWeb REST services
)[https://www.census.gov/data/developers/data-sets/TIGERweb-map-service.html]

    (TIGERweb API directory)[http://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb]
  

## TODOs
* improve API and variable discoverability
* improve API documentation generation
* add geocoding API
* add TIGER/maps APIs with geopandas

## License
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.
