import os
import json
import time
import zlib
import zipfile
import argparse
import urllib.request as request
from datetime import date, datetime
from codecs import decode
from time import sleep


# Internal functions, to ease up parsing data

def _gettime(string):
    "Get GTFS-complaint time value from Gdańsk time value"
    if string.startswith("1899-12-30"):
        return(string.split("T")[-1])
    else:
        s = string.split("T")[-1].split(":")
        h = str(int(s[0]) + 24)
        return(":".join([h, s[1], s[2]]))

def _checkday(day):
    "Check if schedules are avilable for given date"
    timespans = json.loads(decode(request.urlopen("http://91.244.248.19/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/8a546186-396f-4a95-a369-9e3a8f3a4b45/download/stoptimesspan.json").read()))
    timespans = timespans["stopTimesSpan"]
    for agency in timespans:
        start = datetime.strptime(agency["startDate"], "%Y-%m-%d").date()
        end = datetime.strptime(agency["endDate"], "%Y-%m-%d").date()
        if start < end and (not (start <= day <= end)):
            print(agency["agencyId"])
            return(False)
    return(True)

# Parsing Scripts

def stops(day):
    "Parse stops for given day to output/stops.txt GTFS file"
    stops = json.loads(decode(request.urlopen("http://91.244.248.19/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/cd4c08b5-460e-40db-b920-ab9fc93c1a92/download/stops.json").read()))
    stops = stops[day.strftime("%Y-%m-%d")]["stops"]
    file = open("output/stops.txt", "w", encoding="utf-8", newline="\r\n")
    file.write("stop_id,stop_name,stop_lat,stop_lon\n")
    for stop in stops:
        stop_id = str(stop["stopId"])
        stop_name = "\"" + stop["stopDesc"].rstrip().replace("\"", "\"\"").replace("'", "\"\"") + "\""
        stop_lat = str(stop["stopLat"])
        stop_lon = str(stop["stopLon"])
        file.write(",".join([stop_id, stop_name, stop_lat, stop_lon + "\n"]))
    file.close()

def agencies(normalize):
    "Parse agencies to output/agency.txt GTFS file. If normalize is True only two will be created: ZTM Gdańsk and ZKM Gdynia."
    file = open("output/agency.txt", "w", encoding="utf-8", newline="\r\n")
    file.write("agency_id,agency_name,agency_url,agency_timezone,agency_lang\n")
    if normalize:
        file.write("99,ZTM Gdańsk,http://ztm.gda.pl,Europe/Warsaw,pl\n")
        file.write("98,ZKM Gdynia,http://zkmgdynia.pl,Europe/Warsaw,pl\n")
    else:
        agencies = json.loads(decode(request.urlopen("http://91.244.248.19/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/8b3aa347-3bb7-4c58-9113-d47458ec1fc3/download/agency.json").read()))
        agencies = agencies["agency"]
        for agency in agencies:
            agency_id = str(agency["agencyId"])
            agency_name = agency["agencyName"]
            agency_url = agency["agencyUrl"]
            agency_timezone = agency["agencyTimezone"]
            agency_lang = agency["agencyLang"]
            file.write(",".join([agency_id, agency_name, agency_url, agency_timezone, agency_lang + "\n"]))
    file.close()

def routes(day, normalize, extend):
    "Parse routes for given day to output/routes.txt GTFS file. If normalize is True, then agency_id will be filtered to ZTm or ZKM."
    file = open("output/routes.txt", "w", encoding="utf-8", newline="\r\n")
    file.write("agency_id,route_id,route_short_name,route_long_name,route_type,route_color,route_text_color\n")
    routes = json.loads(decode(request.urlopen("http://91.244.248.19/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/4128329f-5adb-4082-b326-6e1aea7caddf/download/routes.json").read()))
    routes = routes[day.strftime("%Y-%m-%d")]["routes"]
    routeslist = []
    for route in routes:
        agency_id = str(route["agencyId"])
        route_id = str(route["routeId"])
        route_short_name = route["routeShortName"]
        route_long_name = route["routeLongName"] if route["routeShortName"] != route["routeLongName"] else ""
        if agency_id == "2":
            #Gdańsk Trams
            route_type = "0"
            route_color = "BB0000,FFFFFF"
        elif agency_id == "5":
            #Gdynia Trolleybuses
            route_type = "800"
            route_color = "11CC11,000000"
        elif route_short_name.startswith("N"):
            #Night Buses
            route_type = "3"
            route_color = "000000,FFFFFF"
        elif not route_short_name.isnumeric():
            #Express Busses
            route_type = "3"
            route_color = "FFCC22,000000"
        else:
            #Normal Busses
            route_type = "3"
            route_color = "2222BB,FFFFFF"
        if normalize:
            if int(agency_id) < 5: agency_id = "99"
            else: agency_id = "98"
        file.write(",".join([agency_id, route_id, route_short_name, route_long_name, route_type, route_color + "\n"]))
        routeslist.append(route_id)
    file.close()
    return(routeslist)

def times(day, routes):
    "Parse stop_times for given day to output/stop_times.txt and output/trips.txt GTFS file"
    fileTimes = open("output/stop_times.txt", "w", encoding="utf-8", newline="\r\n")
    fileTimes.write("trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type\n")
    fileTrips = open("output/trips.txt", "w", encoding="utf-8", newline="\r\n")
    fileTrips.write("service_id,route_id,trip_id\n")
    day = day.strftime("%Y-%m-%d")
    for route in routes:
        triplist = []
        sleep(0.1)
        #Space out calls to schedules server. This reduces risk of getting a TimeOut error
        #print("DEBUG: Requesting day %s, route %s" % (day, route))
        times = json.loads(decode(request.urlopen("http://87.98.237.99:88/stopTimes?date=%s&routeId=%s" % (day, route), timeout=90).read()))
        times = times["stopTimes"]
        for time in times:
            route_id = str(time["routeId"])
            trip_id = "-".join([route_id, time["busServiceName"], str(time["order"])])
            stop_id = str(time["stopId"])
            stop_sequence = str(time["stopSequence"])
            arrival_time = _gettime(time["arrivalTime"])
            departure_time = _gettime(time["departureTime"])
            if time["virtual"] == 1 or time["nonpassenger"] == 1:
                pd_type = "1,1"
            else:
                pd_type = "0,0"
            fileTimes.write(",".join([trip_id, arrival_time, departure_time, stop_id, stop_sequence, pd_type + "\n"]))
            if trip_id not in triplist:
                triplist.append(trip_id)
                fileTrips.write(",".join([day, route_id, trip_id + "\n"]))
    fileTrips.close()
    fileTimes.close()

def calendar(day):
    "Create calendar_dates file for provided day"
    day = day.strftime("%Y-%m-%d")
    file = open("output/calendar_dates.txt", "w", encoding="utf-8", newline="\r\n")
    file.write("date,service_id,exception_type\n")
    file.write("%s,%s,1\n" % (day.replace("-", ""), day))
    file.close()

def feedinfo(day):
    "Create feed_info in output/feed_info.txt to fulfil licencing needs"
    day = day.strftime("%Y-%m-%d")
    file = open("output/feed_info.txt", "w", encoding="utf-8", newline="\r\n")
    file.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date,feed_version\n")
    file.write("Zarząd Transportu Miejskiego w Gdańsku,http://91.244.248.19/organization/ztm-gdansk,pl,%s,%s,%s\n" % (day.replace("-", ""), day.replace("-", ""), date.today().strftime("%Y-%m-%d")))
    file.close()

# Utility Scripts

def cleanup():
    "Cleans output/ directory before parsing."
    if not os.path.exists("output"): os.mkdir("output")
    for file in [os.path.join("output", x) for x in os.listdir("output")]: os.remove(file)

def zip():
    "Zips the content of output/*.txt to gtfs.zip"
    with zipfile.ZipFile("gtfs.zip", mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file in os.listdir("output"):
            if file.endswith(".txt"):
                archive.write("output/" + file, arcname=file)

# Main Funcionlity

def gdanskgtfs(day=date.today(), normalize=False, extend=False):
    if _checkday(day):
        print("Cleaning up output/ dir")
        cleanup()

        print("Parsing agencies")
        agencies(normalize)

        print("Parsing stops")
        stops(day)

        print("Parsing routes")
        rlist = routes(day, normalize, extend)

        print("Parsing stop_times")
        times(day, rlist)

        print("Creating calendar and feed_info")
        calendar(day)
        feedinfo(day)

        print("Zipping to gtfs.zip")
        zip()
    else:
        print("Error! Full schedules are not available for date %s!" % day.strftime("%Y-%m-%d"))

if __name__ == "__main__":
    st = time.time()
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-n", "--normalize", action="store_true", required=False, dest="normalize", help="normalize agencies to ZTM Gdańsk and ZKM Gdynia")
    argprs.add_argument("-e", "--extend", action="store_true", required=False, dest="extend", help="use google's extended route types")
    argprs.add_argument("-d", "--day", default="", required=False, metavar="YYYY-MM-DD", dest="day", help="date for which schedules should be downloaded, if not today")
    args = vars(argprs.parse_args())
    if args["day"]: day = datetime.strptime(args["day"], "%Y-%m-%d").date()
    else: day = date.today()
    print("""
  __                    __ ___ _  __
 /__  _|  _. ._   _ |  /__  | |_ (_
 \_| (_| (_| | | _> |< \_|  | |  __)
    """)
    print("Downloading schedules for %s" % day.strftime("%Y-%m-%d"))
    gdanskgtfs(day, args["normalize"], args["extend"])
    print("=== Done! In %s sec. ===" % round(time.time() - st, 3))
