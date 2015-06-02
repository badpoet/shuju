__author__ = 'badpoet'
import sys
sys.path.append(".")

import wlib.clients
import wlib.clients.mongo
from wlib.weathercncollector import WeatherCnWrapper
from datetime import datetime
import os
import json
import codecs

conf = wlib.clients.get_conf()
col = wlib.clients.mongo.connection(conf)["raw"]
wrapper = WeatherCnWrapper(conf)

dir = sys.argv[1]
if dir[-1] != "/":
    dir += "/"
file_name_list = os.listdir(dir)
gk_dict = {}
for info in codecs.open("resources/city_gib.txt", "r", "utf8"):
    cid, p, d, c, lat, long = info.strip().split()
    gk_dict[cid] = wrapper.make_gk(float(lat), float(long))

file_name_list.sort()
for each in file_name_list:
    if each.find("pm") >= 0:
        continue
    print each
    file_name = dir + each
    year = 2015
    month = int(each[0:2])
    date = int(each[2:4])
    hour = int(each[4:6])
    minute = int(each[6:8])
    wrapper.get_time = lambda: datetime(year, month, date, hour, minute)
    f = codecs.open(file_name, "r", "utf8")
    cnt = 0
    for line in f.readlines():
        if not line: continue
        obj = json.loads(line)
        obj["geo_key"] = gk_dict[obj["city"]]
        if wrapper.accept(obj):
            cnt += 1
    print cnt, "docs accepted"
