#-*- coding: utf8 -*-

__author__ = 'badpoet'

import json
import requests
from collector import WeatherComClient, WrongPage
from time import sleep
import codecs

first_4_prov = {"10101", "10102", "10103", "10104"}

class WeatherComInfo(object):

    def __init__(self):
        self.f = codecs.open("city_codes.txt", "w", "utf8")
        self.headers = {
            "Host": "www.weather.com.cn",
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36",
            "Referer": "http://www.weather.com.cn/live/",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Accept-Language": "zh-CN,zh;q=0.8,ru;q=0.6,zh-TW;q=0.4,en;q=0.2"
        }

    def __del__(self):
        self.f.close()

    def get_province_list(self):
        return {"10101": u"北京",
                "10102": u"上海",
                "10103": u"天津",
                "10104": u"重庆",
                "10105": u"黑龙江",
                "10106": u"吉林",
                "10107": u"辽宁",
                "10108": u"内蒙古",
                "10109": u"河北",
                "10110": u"山西",
                "10111": u"陕西",
                "10112": u"山东",
                "10113": u"新疆",
                "10114": u"西藏",
                "10115": u"青海",
                "10116": u"甘肃",
                "10117": u"宁夏",
                "10118": u"河南",
                "10119": u"江苏",
                "10120": u"湖北",
                "10121": u"浙江",
                "10122": u"安徽",
                "10123": u"福建",
                "10124": u"江西",
                "10125": u"湖南",
                "10126": u"贵州",
                "10127": u"四川",
                "10128": u"广东",
                "10129": u"云南",
                "10130": u"广西",
                "10131": u"海南",
                "10132": u"香港",
                "10133": u"澳门",
                "10134": u"台湾"}

    def get_district_list(self, pid):
        sleep(2)
        url = "http://www.weather.com.cn/data/city3jdata/provshi/%s.html" % str(pid)
        r = requests.get(url, headers=self.headers)
        assert r.status_code == 200
        return json.loads(r.text)

    def get_station_list(self, pid, did):
        sleep(2)
        url = "http://www.weather.com.cn/data/city3jdata/station/%s.html" % (str(pid) + str(did),)
        r = requests.get(url, headers=self.headers)
        assert r.status_code == 200
        return json.loads(r.text)

    def get_city_info(self):
        p_list = self.get_province_list()
        for pid in p_list:
            d_list = self.get_district_list(pid)
            for did in d_list:
                s_list = self.get_station_list(pid, did)
                for sid in s_list:
                    if pid in first_4_prov:
                        cid = pid + sid + "00"
                    else:
                        cid = pid + did + sid
                    line = cid + " " + p_list[pid] + " " + \
                           d_list[did] + " " + \
                           s_list[sid] + "\n"
                    print line.strip()
                    self.f.write(line)

if __name__ == "__main__":
    wci = WeatherComInfo()
    wci.get_city_info()
