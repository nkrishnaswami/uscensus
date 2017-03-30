# Census API helpers
This module fetches metadata for the Census API to make it easier to
find relevant APIs and variables to query, and returns data into a Pandas dataframe.

See the example notebook for basic usage.

## API Key
You will need a census API key to call the APIs.  You can request one here:  
  http://api.census.gov/data/key_signup.html

In the example notebook, I read it using configparser from a config file, `~/.census`.

## TODOs
* improve API and variable discoverability
* improve API documentation generation
* add geocoding API
* add TIGER/maps APIs with geopandas

## License
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.
