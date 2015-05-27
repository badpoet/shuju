__author__ = 'badpoet'

import requests
import json
import codecs
from time import sleep
from datetime import datetime

MAX_TRY = 3


class WrongPage(Exception):

    def __str__(self):
        return "Not the correct page."


class WeatherCnClient(object):

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
        while cnt < MAX_TRY:
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

    def fetch_all(self, wrapper = None):
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
                    if wrapper:
                        obj["geo_key"] = wrapper.make_gk(lat, long)
                        wrapper.accept(obj)
                        wrapper.log(pr + json.dumps(obj, ensure_ascii=False), type="log")
                except Exception, e:
                    print e
                    print obj
            else:
                print pr, "failed on", cid
                if wrapper:
                    wrapper.log(pr + "failed on " + cid, type="err")


class Pm25InClient(object):

    def __init__(self):
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

    def fetch_all(self, type_key):
        MAX_TRY = 3
        params = { "token": self.token, "stations": "no" }
        res25 = []
        res10 = []
        ts = "weather/data/" + datetime.now().strftime("%m%d%H")
        for each in self.city_list:
            print each
            params["city"] = each
            cnt = 0
            while cnt < MAX_TRY:
                cnt += 1
                try:
                    if type_key == "pm2_5":
                        r = requests.get(self.pm25, params=params).json()[0]
                    else:
                        r = requests.get(self.pm10, params=params).json()[0]
                    if "error" in r:
                        raise WrongPage()
                    res10.append({
                        "city": each,
                        "type": type_key,
                        "value": float(r[type_key]),
                        "time": r["time_point"].split("T")[1][:-1],
                        "date": r["time_point"].split("T")[0]
                    })
                except WrongPage, e:
                    print "Failed on", each
                except Exception, e:
                    print e
                    cnt += 1

        json.dump(res25, codecs.open(ts + type_key, "w", "utf8"), ensure_ascii=False)
        print "one cycled"


def sleep_to_next_hour():
    t = (65 - datetime.now().minute) * 60.0
    print "sleep", t, "seconds"
    sleep(t)

if __name__ == "__main__":
    pm25 = Pm25InClient()
    pm25.fetch_all("pm2_5")
    pm25.fetch_all("pm10")
    sleep_to_next_hour()
