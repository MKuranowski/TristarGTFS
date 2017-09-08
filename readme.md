# GdanskGTFS

## Description
Creates GTFS data feed for Gdańsk and Gdynia.
Data comes from [Open Gdańsk CKAN project](http://91.244.248.19/dataset/tristar).

## Some precautions

- Produced feed will use extended type 800 for trolleybuses.
- Because of issues with trips handling by ZTM Gdańsk, their trip_id is omitted. Trip_ids are created using this scheme `[route_id]-[busServiceName]-[order]`. This makes it impossible to use RT data with produced GTFS feed.
- Again, because of how ZTM Gdańsk provides data, GTFS is only valid for one day. This might get fixed later.
- Agencies provided by ZTM Gdańsk are actually line operators. This data is too deatiled and will misslead users, so if the feed is going to be used for public transport users, please use the `--normalize` option.
- Sometimes, the ZTM Gdańsk database with stop_times will refuse to cooperate and timeout error is raised.
- ZTM Gdańsk shares their data under [CC BY](http://www.opendefinition.org/licenses/cc-by) license, you have to give a credit, according to [ZTM Gdańsk usage terms (Polish)](http://91.244.248.19/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/bdf70c01-ad02-4317-bc61-31a3ba3b1bba/download/regulamin-korzystania-z-danych.pdf). The ZTM Gdańsk name, link to their page on Open Gdańsk project and data download date will all be included in feed_info.txt file (under columns `feed_provider_name, feed_publisher_url and feed_version`).


## Running

### First Launch

Of course you will need [Python3](https://www.python.org), but no additonal modules all required.
Just launch `python3 gdanskgtfs.py -n`.

### Configuration

There are three command line options:

- **--help / -h** Prints all available options with their descriptions,

- **--normalize / -n** Fixes agencies, as the ones provided by ZTM Gdańsk will mislead feed users,

- **--day / -d YYYY-MM-DD** Tries to download schedules for given date, insted of default today date.

## License

*GdanskGTFS* is provided under the MIT license, included in the `license.md` file.
