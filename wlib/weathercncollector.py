__author__ = 'badpoet'

import codecs
import json
from time import sleep
from datetime import datetime, timedelta

import requests

from exception import WrongPage
import clients
import clients.mongo

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
        "moderate snow": 3,
        "snow": 3,
        "snow flurry": 4
    }

    def __init__(self, conf=None):
        if not conf:
            return
        self.db = clients.mongo.connection(conf)
        self.tz_offset = conf.get_int("app.tz_offset")
        self.col = self.db["raw"]

    def accept(self, obj):
        gk = obj["geo_key"]
        # timestamp = datetime.now().strftime("%m%d%H%M")
        timestamp = self.guess_date(obj["time"]) + obj["time"][:2] + obj["time"][3:]
        self.accept_wind(gk, timestamp, obj)
        self.accept_rain(gk, timestamp, obj)
        self.accept_temp(gk, timestamp, obj)
        self.accept_humid(gk, timestamp, obj)
        self.accept_aqi(gk, timestamp, obj)

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
            self.update("wind", gk, timestamp, (w[0], w[1]))
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

    def update(self, base, gk, timestamp, value):
        lat, long = gk.split("+")
        lat = float(lat)
        long = float(long)
        self.col.insert({
            "lat": lat,
            "long": long,
            "timestamp": timestamp,
            "type_key": base,
            "value": value
        })

    def make_gk(self, lat, long):
        lat = int(round(lat * 100)) / 100.0
        long = int(round(long * 100)) / 100.0
        return str(lat) + "+" + str(long)

    def guess_date(self, t, base=datetime.now()):
        base += timedelta(self.tz_offset)
        t1 = datetime(base.year, base.month, base.day, int(t[:2]), int(t[3:]))
        if t1 > base:
            t1 = t1 - timedelta(days=1)
        return t1.strftime("%m%d")


class WeatherCnCollector(object):

    MAX_TRY = 3

    def __init__(self):
        self.headers = {
            "Host": "d1.weather.com.cn",
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36",
            "Referer": "http://www.weather.com.cn/weather1d/101010100.shtml",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Accept-Language": "zh-CN,zh;q=0.8,ru;q=0.6,zh-TW;q=0.4,en;q=0.2"
        }
        self.base_url = "http://d1.weather.com.cn/sk_2d/%s.html"
        self.city_tuples = []
        f = codecs.open("resources/city_gib.txt", "r", "utf8")
        for each in f.readlines():
            cid, p, d, s, lat, long = each.strip().split(" ")
            self.city_tuples.append((cid, p, d, s, lat, long))
        f.close()

    def make_url(self, cid):
        return self.base_url % str(cid)

    def make_headers(self, cid):
        return self.headers

    def analyse_html(self, html):
        try:
            return json.loads(html[13:])
        except Exception, e:
            raise WrongPage()

    def fetch_once(self, url, headers, timeout=5):
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            raise WrongPage()
        return self.analyse_html(resp.text)

    def fetch_raw(self, cid):
        return self.fetch_once(self.make_url(cid), self.make_headers(cid))

    def fetch_page(self, cid):
        cnt = 0
        silence = 30
        last_exception = None
        timeout = 2
        while cnt < WeatherCnCollector.MAX_TRY:
            try:
                return self.fetch_once(self.make_url(cid), self.make_headers(cid), timeout)
            except requests.Timeout, e:
                cnt += 1
                timeout += 5
                last_exception = e
            except Exception, e:
                print "Are we banned?"
                cnt += 1
                silence += 30
                sleep(silence)
                last_exception = e
        print last_exception

    def fetch_all(self, wrapper):
        city_tuples = self.city_tuples
        tot = len(city_tuples)
        cnt = 0
        for cid, p, d, s, lat, long in city_tuples:
            cnt += 1
            sleep(0.5)
            obj = self.fetch_page(cid)
            pr = str(cnt) + "/" + str(tot) + " "
            if obj:
                try:
                    print pr, obj["cityname"].encode("utf8"), obj["time"].encode("utf8")
                    obj["lat"] = lat
                    obj["long"] = long
                    obj["geo_key"] = wrapper.make_gk(float(lat), float(long))
                    wrapper.accept(obj)
                except Exception, e:
                    print e
                    print obj
            else:
                print pr, "failed on", cid

if __name__ == "__main__":
    conf = clients.get_conf()
    wrapper = WeatherCnWrapper(conf)
    collector = WeatherCnCollector()
    while True:
        collector.fetch_all(wrapper)
