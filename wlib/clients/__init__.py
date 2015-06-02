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
for info in codecs.open("resources/city_gib.txt", "r", "utf8"):
    cid, p, d, c, lat, long = info.strip().split()
    gk_dict[cid] = make_gk(float(lat), float(long))


from datetime import datetime
def stamp_to_obj(stamp):
    year = int(stamp[0:4])
    month = int(stamp[4:6])
    day = int(stamp[6:8])
    hour = int(stamp[8:10])
    minute = int(stamp[10:12])
    return datetime(year, month, day, hour, minute)

