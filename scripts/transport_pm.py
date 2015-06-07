__author__ = 'badpoet'

import sys
sys.path.append(".")

import wlib.clients.mongo
from wlib.pmincollector import PmInWrapper
import os
import json
import codecs

conf = wlib.clients.get_conf()
col = wlib.clients.mongo.connection(conf)["raw"]
wrapper = PmInWrapper(conf)

dir = sys.argv[1]
if dir[-1] != "/":
    dir += "/"
file_name_list = os.listdir(dir)

file_name_list.sort()
for each in file_name_list:
    if not each.find("pm") >= 0:
        continue
    print each
    file_name = dir + each
    f = codecs.open(file_name, "r", "utf8")
    if each.find("pm2_5") >= 0:
        type_key = "pm2_5"
    else:
        type_key = "pm10"
    each = each[0:6]
    year = 2015
    month = int(each[0:2])
    date = int(each[2:4])
    hour = int(each[4:6])
    cnt = 0
    obj = json.load(f)
    for each in obj:
        cnt += 1
        wrapper.accept(each)
    f.close()
    print cnt, "docs accepted"

