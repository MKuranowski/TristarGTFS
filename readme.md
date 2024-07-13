# ⚠️ Deprecation warning

This script should no longer be used. Use GTFS dataset published by the agencies:
[ZTM Gdańsk](https://ckan.multimediagdansk.pl/dataset/tristar/resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96) and
[ZKM Gdynia](https://www.otwartedane.gdynia.pl/pl/dataset/informacje-o-rozkladach-jazdy-i-lokalizacji-przystankow).

# TristarGTFS

## Description
Creates GTFS/GTFS-Realtime data feed for Gdańsk and Gdynia.
Data comes from [Open Gdańsk project](https://ckan.multimediagdansk.pl/dataset/tristar) and [Gdynia open data project](http://otwartedane.gdynia.pl/dataset?tags=GTFS).
The script can also generate shapes.txt for Gdynia shcedules, which whill in turn use data from [© OpenStreetMap contributors](https://www.openstreetmap.org/copyright).

### Some precautions

- Produced feed will use extended type 800 for trolleybuses.
- Data is created up to last day, when schedules are avialable for all agencies.
- Both ZTM Gdańsk and ZKM Gdynia data sources share their data under [CC BY](http://www.opendefinition.org/licenses/cc-by) license, so you have to credit them.
  This can be done by exposing the data from `feed_info.txt` file, columns `feed_provider_name`, `feed_publisher_url` and `feed_version`.

### Ids
Because `route_id`s, `trip_id`s and `service_id`s could collide
between ZTM Gdańsk and ZKM Gdynia data sources,
each `route_id`, `trip_id` and `service_id` is prefixed with
`1:` for ZTM Gdańsk data and `2:` for ZKM Gdynia data.

As of the time writing [the stop merge table](https://ckan.multimediagdansk.pl/dataset/tristar/resource/f8a5bedb-7925-40c9-8d66-dbbc830939b1)
references unexisting stops — it is **not** used.
All ZTM Gdańsk `stop_id`s are asserted to be *< 30000* and all ZKM Gdynia `stop_id`s are asserted to be *>= 30000*.

## Running

### First Launch

Of course you will need [Python3](https://www.python.org) (version 3.6 or later), with these modules:
- [Requests](https://2.python-requests.org/en/master/),
- [pyroutelib3](https://pypi.org/project/pyroutelib3/) >= 1.3,
- [rdp](https://pypi.org/project/rdp/),
- [gtfs-realtime-bindings](https://pypi.org/project/gtfs-realtime-bindings/) >= 0.0.5.

Before launching install required modules with `pip3 install -r requirements.txt`
Each script can launch without any command line options, but you may want to take a look at them.

### Static GTFS - tristargtfs.py
`python3 tristargtfs.py` - Creates GTFS file in `gtfs.zip` without shapes.

Options:
- **-o / --output-file TARGET-PATH-OF-GTFS.zip**: Destination path of the gtfs archive,
- **-s / --shapes**: Use OSM to gerenate shapes for ZKM Gdynia + copy ZTM Gdańsk shapes.

### Realtime GTFS - tristargtfs_realtime.py
`python3 tristargtfs_realtime.py` - Creates binary GTFS-RT file in `gtfs-rt.pb` for [mkuran.pl Tristar GTFS](https://mkuran.pl/feed/)

Options:
- **-o / --output-file TARGET-PATH-OF-GTFS-RT.pb**: Destination of gtfs-realtime file,
- **--gtfs PATH-OR-URL-TO-GTFS**: Path/URL to the Tristar GTFS file to use as a base for RT data,
- **--readable**: Output data to a human-readable protobuff instead of binary one,
- **--debug**: Do some more printing when there are issues with encountered data,
- **-l / --loop**: Run the script in a loop - autmoatically update the taget-file,
- **-p / --peroid SECONDS**: How often should the target-file should be updated (for *--loop*, default 30s),
- **--gtfs-check-peroid SECONDS**: How often should the script check if gtfs file has changed (for *-loop*, default 1800s/30min).

## License

*TristarGTFS* is provided under the MIT license, included in the `license.md` file.
