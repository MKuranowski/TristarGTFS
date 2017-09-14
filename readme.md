# GdanskGTFS

## Description
Creates GTFS data feed for Gdańsk and Gdynia.
Data comes from [Open Gdańsk CKAN project](http://91.244.248.19/dataset/tristar).

## Some precautions

- Produced feed will use extended type 800 for trolleybuses.
- Data is created up to last day, when schedules are avialable for all agencies.
- Agencies provided by ZTM Gdańsk are actually line operators. This data is too deatiled and will misslead users, so if the feed is going to be used for public transport users, please use the `--normalize` option.
- ZTM Gdańsk shares their data under [CC BY](http://www.opendefinition.org/licenses/cc-by) license, you have to give a credit, according to [ZTM Gdańsk usage terms (Polish)](http://91.244.248.19/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/bdf70c01-ad02-4317-bc61-31a3ba3b1bba/download/regulamin-korzystania-z-danych.pdf). The ZTM Gdańsk name, link to their page on Open Gdańsk project and data download date will all be included in feed_info.txt file (under columns `feed_provider_name, feed_publisher_url and feed_version`).

### Ids
Because stops and routes are publish seperately for each date, the stop_id and route_id in created GTFS are different from those used by ZTM.
To help you with using RT data, if you use the `--tables` option, a file tables.json will be created, with mapping of ids used by gdańsk to ids in the GTFS.
The key of each table is created using scheme `YYYY-MM-DD-[ZTM ID]`, and the value is the GTFS id.


Also trip_id in Gdańsk represents a route variant, so the trip_id in GTFS is created using using this scheme: `R[route_id]D[date]T[trip_id]S[busServiceName]O[order]`.

## Running

### First Launch

Of course you will need [Python3](https://www.python.org), with [Requests](http://docs.python-requests.org/en/master/) module.
Befor launching  install required modules with `pip3 install -r requirements.txt`
And then simply `python3 gdanskgtfs.py -n`.

Warning: depending on your internet connection, GTFS creation can take up to 20 minutes!

### Configuration

There are three command line options:

- **--help / -h** Prints all available options with their descriptions,

- **--normalize / -n** Fixes agencies, as the ones provided by ZTM Gdańsk will mislead feed users,

- **--day / -d YYYY-MM-DD** Tries to download schedules starting from given date, insted of those starting today,

- **--tables / -t** Creates tables.json file with mapping from route & stop ZTM IDs to those used in the GTFS,

- **--extend / -e** Aritficially extends dates avialable in the feed to 30 days, using latest avialable schedules for each weekday.

## License

*GdanskGTFS* is provided under the MIT license, included in the `license.md` file.
