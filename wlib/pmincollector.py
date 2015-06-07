__author__ = 'badpoet'

from clients import gkc_dict, city_std, get_conf
import json
import codecs
import requests
from exception import WrongPage
from time import sleep
import clients.mongo
from datetime import datetime

class PmInCollector(object):

    def __init__(self, conf=None):
        self.token = "67rst15R8Th4h9sREX7D"
        self.pm25 = "http://www.pm25.in/api/querys/pm2_5.json"
        self.pm10 = "http://www.pm25.in/api/querys/pm10.json"
        self.cities = "http://www.pm25.in/api/querys.json"
        self.city_list = json.load(codecs.open("resources/pm25cities.json", "r", "utf8"))["cities"]

    def get_cities(self):
        '''
        params = { "token": self.token }
        resp = requests.get(self.cities, params=params)
        return resp.json()
        '''
        return None

    def fetch_all(self, type_key, wrapper):
        MAX_TRY = 3
        params = { "token": self.token, "stations": "no" }
        res = []
        ts = "weather/data/" + datetime.now().strftime("%m%d%H")
        idx = 0
        for each in self.city_list:
            idx += 1
            print idx, each.encode("utf8")
            params["city"] = each
            cnt = 0
            while cnt < MAX_TRY:
                cnt += 1
                try:
                    if type_key == "pm2_5":
                        r = requests.get(self.pm25, params=params).json()
                    else:
                        r = requests.get(self.pm10, params=params).json()
                    if "error" in r:
                        raise WrongPage()
                    r = r[0]
                    wrapper.accept({
                        "city": each,
                        "type": type_key,
                        "value": float(r[type_key]),
                        "time": r["time_point"].split("T")[1][:-1],
                        "date": r["time_point"].split("T")[0]
                    })
                    break
                except WrongPage, e:
                    print "Failed on", each.encode("utf8")
                    break
                except Exception, e:
                    print e
                    cnt += 1

        json.dump(res, codecs.open(ts + type_key, "w", "utf8"), ensure_ascii=False)
        print "one cycled"


    def sleep_to_next_hour(self):
        t = (75 - datetime.now().minute) * 60.0
        print "sleep", t, "seconds"
        sleep(t)


class PmInWrapper(object):

    def __init__(self, conf):
        if not conf:
            return
        self.db = clients.mongo.connection(conf)
        self.tz_offset = conf.get_int("app.tz_offset")
        self.col = self.db["raw"]
        self.col.ensure_index([("type_key", 1), ("lat", 1), ("long", 1), ("timestamp", 1)])
        self.col.ensure_index([("type_key", 1), ("lat", 1), ("long", 1), ("timestamp", -1)])

    def get_loc(self, city):
        try:
            if city not in city_std:
                if city not in gkc_dict:
                    return None
                else:
                    return gkc_dict[city]
            else:
                return gkc_dict[city_std[city]]
        except Exception, e:
            print city
            print e

    def make_timestamp(self, date, time):
        return date[0:4] + date[5:7] + date[8:10] + time[0:2] + "00"

    def accept(self, obj):
        stamp = self.make_timestamp(obj["date"], obj["time"])
        lat, long = self.get_loc(obj["city"])
        self.col.update({
            "timestamp": stamp,
            "lat": lat,
            "long": long,
            "type_key": obj["type"]
        }, {
            "$set": {"value": obj["value"]}
        }, upsert=True)

if __name__ == "__main__":
    pm25 = PmInCollector()
    wrapper = PmInWrapper(conf=clients.get_conf())
    while True:
        pm25.fetch_all("pm2_5", wrapper)
        pm25.fetch_all("pm10", wrapper)
        pm25.sleep_to_next_hour()