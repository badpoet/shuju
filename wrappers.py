__author__ = 'badpoet'

from datetime import datetime, timedelta
import pymongo

LIST_LEN = 5

class WeatherCnWrapper(object):

    rain_table = {
        "no live": 0,
        "sunny": 0,
        "cloudy": 0,
        "overcast": 0,
        "foggy": 0,
        "haze": 0,
        "dust": 0,
        "duststorm": 0,
        "sleet": 0,
        "sand": 0,
        "shower": 1,
        "thundershower": 2,
        "thundershower with hail": 2,
        "light rain": 3,
        "moderate rain": 4,
        "heavy rain": 5,
        "light snow": 2,
        "snow": 3,
        "snow flurry": 4
    }

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
        # timestamp = datetime.now().strftime("%m%d%H%M")
        timestamp = obj["date"] + obj["time"][:2] + obj["time"][3:]
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
            self.update("wind", gk, timestamp, w[0], w[1])
        except Exception, e:
            return

    def accept_rain(self, gk, timestamp, obj):
        try:
            w = obj["weathere"].lower().strip()
            n = WeatherCnWrapper.rain_table[w]
            self.update("rain", gk, timestamp, n)
        except Exception, e:
            s = obj.get("weathere", "").lower().strip()
            if s:
                print s
            return

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
        data_list = tuple([timestamp, base, lat, long] + list(args))
        gk = (lat, long)
        if gk not in virtual_db[base]:
            virtual_db[base][gk] = []
        virtual_db[base][gk].append(data_list)

    def make_gk(self, lat, long):
        lat = int(round(lat * 100)) / 100.0
        long = int(round(long * 100)) / 100.0
        return str(lat) + "+" + str(long)

import os
import sys
import codecs
import json

virtual_db = {
    "temp": {},
    "aqi": {},
    "humid": {},
    "rain": {},
    "wind": {}
}

def guess_date(date, hour, t):
    base = datetime(2015, int(date[:2]), int(date[2:]), int(hour), 0) + timedelta(hours=8)
    t1 = datetime(base.year, base.month, base.day, int(t[:2]), int(t[3:]))
    t2 = t1 + timedelta(days=1)
    t3 = t1 - timedelta(days=1)
    if abs(t1 - base) > abs(t2 - base):
        if abs(t2 - base) > abs(t3 - base):
            return t3.strftime("%m%d")
        else:
            return t2.strftime("%m%d")
    else:
        if abs(t1 - base) > abs(t3 - base):
            return t3.strftime("%m%d")
        else:
            return t1.strftime("%m%d")


def local(dir):
    if dir[-1] != "/":
        dir = dir + "/"
    flist = os.listdir(dir)
    wrapper = WeatherCnWrapper()
    gk_dict = {}
    for info in codecs.open("resources/city_gib.txt", "r", "utf8"):
        cid, p, d, c, lat, long = info.strip().split()
        gk_dict[cid] = wrapper.make_gk(float(lat), float(long))
    for each in flist:
        date = each[:4]
        f = codecs.open(dir + each, "r", "utf8")
        v = dict()
        f = codecs.open(dir + each, "r", "utf8")
        maj = int(each[4:6])
        for ol in f.readlines():
            if not ol: continue
            if ol.find("failed") >= 0: continue
            obj = json.loads(ol)
            hour = obj["time"][:2]
            if not hour or hour == u'\u6682\u65e0' or hour == "99": continue
            obj["date"] = guess_date(date, maj, obj["time"])
            obj["geo_key"] = gk_dict[obj["city"]]
            wrapper.accept(obj)
    min_ts = ""
    max_ts = ""
    for b in virtual_db:
        for k in virtual_db[b]:
            virtual_db[b][k].sort(key=lambda x:x[0])
            if virtual_db[b][k][0][0] < min_ts or not min_ts: min_ts = virtual_db[b][k][0][0]
            if virtual_db[b][k][-1][0] > max_ts or not max_ts: max_ts = virtual_db[b][k][-1][0]
    print min_ts, max_ts
    date = datetime(year=2015, month=int(min_ts[:2]), day=int(min_ts[2:4]), hour=int(min_ts[4:6])) + timedelta(hours=1)
    max_date = datetime(year=2015, month=int(max_ts[:2]), day=int(max_ts[2:4]), hour=int(max_ts[4:6]))
    while date <= max_date:
        for b in virtual_db:
            f = open("out/" + date.strftime("%m%d%H") + "-" + b + ".txt", "w")
            print date.strftime("%m%d%H") + "-" + b
            ts = date.strftime("%m%d%H%M")
            for k in virtual_db[b]:
                lat, long = k
                for u in range(len(virtual_db[b][k]) - 1):
                    if virtual_db[b][k][u][0] <= ts and virtual_db[b][k][u + 1][0] >= ts:
                        data1 = virtual_db[b][k][u][4:]
                        data2 = virtual_db[b][k][u + 1][4:]
                        data = [(float(x) + float(y)) / 2 for x, y in zip(data1, data2)]
                        f.write("\t".join(map(str, [lat, long] + data)) + "\n")
                        break
            f.close()
        date += timedelta(hours=1)

    return virtual_db


if __name__ == "__main__":
    dir = sys.argv[1].strip()
    local(dir)