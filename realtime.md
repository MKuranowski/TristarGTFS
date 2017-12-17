# Connecting GTFS with realtime data

ZTM Gdańsk also has repository with realtime departures from stops,
so I wrote this file to show how you can use dynamic data in coordination with GTFS.

The document will only focus on matching **one** StopTime event, but you can easily extednd it yourself to cover the whole feed.

This document is based on my own experience and ZTM Gdańsk data spec, [available here, in Polish](http://91.244.248.19/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/9a2dd8fd-ffbe-42de-8c6d-9af0f57f51af/download/opis-zbioru-otwarte-dane-ztm-w-gdasku--tristar--2017-05-223.pdf).

## Before you start

Please note that several terms used by ZTM Gdańsk in their datasets does not
correspond to those used in GTFS.

- *trip_id* in ZTM datasets represents a **route variant**. I'll refer to this field *original_trip_id*,
- *stop_id* and *route_id* in GTFS does **not** match with those used by ZTM. See *readme.md* for a full explenation.

  Terms *original_stop_id* and *original_route_id* will refer to values as referenced by ZTM Gdańsk, while *stop_id* and *route_id* will match values from produced GTFS.


## Matching the data

In order to match static StopTime event to realtime data you need these values:

- *original_route_id* and *original_trip_id*. Those can be "extracted" from GTFS *trip_id*, as it's created via this pattern:
 `R[original_route_id]D[date]T[original_trip_id]S[busServiceName]O[order]`;
- *original_stop_id*. This value can be seen in column `original_stop_id` of stop_times.txt;
- *arrival_time*, converted to human-readable representation and without seconds (e.g. `25:11:00` → `01:11`).

The RT departures are located at `http://87.98.237.99:88/delays?stopId=<original_stop_id>`.

## Example

Let's say, that you would like to find RT data for this StopTime event:

```
trip_id,arrival_time,departure_time,stop_id,original_stop_id,stop_sequence,pickup_type,drop_off_type
<...>
R3D2017-12-17T11S003-01O2,12:17:00,12:17:00,2002-0,2002,16,0,0
<...>
```

From the *trip_id*, you would interpolate this data:
- *original_route_id*: **3**,
- *original_trip_id*: **11**.

Other required fileds would look like this:
- *original_stop_id*: **2002**,
- *arrival_time*: **12:17**.

Then, you would make a GET request for this address: `http://87.98.237.99:88/delays?stopId=2002`, and get response that will look like this:

```
{
  "lastUpdate" : "2017-12-17 12:17:28",
  "delay" : [ {
    "id" : "T11R3",
    "delayInSeconds" : 149,
    "estimatedTime" : "12:19",
    "headsign" : "Stogi Pasanil",
    "routeId" : 3,
    "tripId" : 11,
    "status" : "REALTIME",
    "theoreticalTime" : "12:17",
    "timestamp" : "12:16:59",
    "trip" : 1492588,
    "vehicleCode" : 1121,
    "vehicleId" : 387
  }, <...> ]
}
```

As there should be many objects in the `delay` array, you would have to find one that matches your data like this:

| Required data                 | Object in array |
| ----------------------------- | --------------- |
| original_route_id             | routeId         |
| original_trip_id              | tripId          |
| human-readable arrival_time   | theoreticalTime |

In this example, it would match with the only visible object in the example, but
if you can't match those data with any object in the `delay` array,
you can assume no realtime data is available.

## Description of other RT data

| Realtime Key | Meaning |
| ------------ | ------- |
| id | `T<original_trip_id>R<original_route_id>`, nothing useful |
| delayInSeconds | Delay of vehicle in seconds. If negative, vehicle is running early |
| estimatedTime | Estimated RT arrival for this StopTime |
| headsign | Headsign, trimmed to 17 letters, useless |
| status | Always `REALTIME`, kinda useless |
| timestamp | Timestamp when delay of vehicle has been read |
| trip | ZTM Gdańsk internal trip id, useless |
| vehicleCode | Side number ("label") of vehicle, visible to the end-user |
| vehicleId | Internal id representing vehicle |
