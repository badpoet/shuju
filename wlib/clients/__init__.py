__author__ = 'badpoet'

import pyhocon

conf = pyhocon.ConfigFactory.parse_file("resources/config.hocon")
def get_conf():
    return conf


def make_gk(lat, long):
    lat = int(round(lat * 100)) / 100.0
    long = int(round(long * 100)) / 100.0
    return str(lat) + "+" + str(long)

import codecs

gk_dict = {}
gkc_dict = {}
for info in codecs.open("resources/city_gib.txt", "r", "utf8").readlines():
    cid, p, d, c, lat, long = info.strip().split()
    gk_dict[cid] = make_gk(float(lat), float(long))
    gkc_dict[c] = make_gk(float(lat), float(long))


city_std = {}
for t in codecs.open("resources/city_std.txt", "r", "utf8").readlines():
    a, b = t.strip().split()
    city_std[a] = b


from datetime import datetime, timedelta
def stamp_to_obj(stamp):
    year = int(stamp[0:4])
    month = int(stamp[4:6])
    day = int(stamp[6:8])
    hour = int(stamp[8:10])
    minute = int(stamp[10:12])
    return datetime(year, month, day, hour, minute)


def prev_date(stamp):
    year = int(stamp[0:4])
    month = int(stamp[4:6])
    day = int(stamp[6:8])
    return (datetime(year, month, day, 0, 0) - timedelta(days=1)).strftime("%Y%m%d")


def next_date(stamp):
    year = int(stamp[0:4])
    month = int(stamp[4:6])
    day = int(stamp[6:8])
    return (datetime(year, month, day, 0, 0) + timedelta(days=1)).strftime("%Y%m%d")
