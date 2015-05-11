__author__ = 'badpoet'

from datetime import datetime
import pymongo

LIST_LEN = 5

class WeatherCnWrapper(object):

    def __init__(self, conf=None):
        if not conf:
            return
        db = pymongo.Connection(
            host=conf.get_string('db.mongo.host'),
            port=conf.get_int('db.mongo.port'),
        )[conf.get_string('db.mongo.db')]
        # db.authenticate(
        #     conf.get_string('db.mongo.user'),
        #     conf.get_string('db.mongo.pw'),
        # )
        self.log = db["log"]

    def accept(self, obj):
        gk = obj["geo_key"]
        timestamp = datetime.now().strftime("%m%d%H%M")
        self.accept_wind(gk, timestamp, obj)
        self.accept_rain(gk, timestamp, obj)
        self.accept_temp(gk, timestamp, obj)
        self.accept_humid(gk, timestamp, obj)
        self.accept_aqi(gk, timestamp, obj)

    def log(self, rec, type):
        self.log.insert(dict(type=type, log=rec))

    def accept_wind(self, gk, timestamp, obj):
        try:
            wde = obj["wde"].strip()
            if wde == "N":
                d = (0, -1)
            elif wde == "NE":
                d = (-1, -1)
            elif wde == "E":
                d = (-1, 0)
            elif wde == "SE":
                d = (-1, 1)
            elif wde == "S":
                d = (0, 1)
            elif wde == "SW":
                d = (1, 1)
            elif wde == "W":
                d = (1, 0)
            elif wde == "NW":
                d = (1, -1)
            else:
                raise Exception
            wse = int(obj["WS"].strip()[:-1])
            wsa = float(wse) / (d[0] ** 2 + d[1] ** 2) ** 0.5
            w = (wsa * d[0], wsa * d[1])
        except Exception, e:
            return
        self.update("wind", gk, timestamp, w[0], w[1])

    def accept_rain(self, gk, timestamp, obj):
        pass

    def accept_temp(self, gk, timestamp, obj):
        try:
            temp = float(obj["temp"])
            self.update("temp", gk, timestamp, temp)
        except Exception, e:
            return

    def accept_humid(self, gk, timestamp, obj):
        try:
            humid = obj["sd"].strip()
            if humid[-1] != "%": raise Exception()
            self.update("humid", gk, timestamp, float(humid[:-1]))
        except Exception, e:
            return

    def accept_aqi(self, gk, timestamp, obj):
        try:
            aqi = int(obj["aqi"])
            self.update("aqi", gk, timestamp, aqi)
        except Exception, e:
            return

    def update(self, base, gk, timestamp, *args):
        lat, long = gk.split("+")
        lat = float(lat)
        long = float(long)
        data_list = [timestamp, base, lat, long] + list(args)
        if base + gk not in virtual_db:
            virtual_db[base + gk] = []
        virtual_db[base + gk].append(data_list)

    def make_gk(self, lat, long):
        lat = int(round(lat * 100)) / 100.0
        long = int(round(long * 100)) / 100.0
        return str(lat) + "+" + str(long)

import os
import sys
import codecs
import json

virtual_db = {}

def local():
    dir = sys.argv[1].strip()
    if dir[-1] != "/":
        dir = dir + "/"
    flist = os.listdir(dir)
    wrapper = WeatherCnWrapper()
    gk_dict = {}
    for info in codecs.open("resources/city_gib.txt", "r", "utf8"):
        cid, p, d, c, lat, long = info.strip().split()
        gk_dict[cid] = wrapper.make_gk(float(lat), float(long))
    for each in flist:
        f = codecs.open(dir + each, "r", "utf8")
        for ol in f.readlines():
            if not ol: continue
            if ol.find("failed") >= 0: continue
            obj = json.loads(ol)
            obj["geo_key"] = gk_dict[obj["city"]]
            wrapper.accept(obj)
    for k in virtual_db:
        virtual_db[k].sort(key=lambda x:x[0])
    print len(virtual_db.keys())


if __name__ == "__main__":
    local()