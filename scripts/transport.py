__author__ = 'badpoet'

import os
import sys
import pyhocon
import pymongo

conf = pyhocon.ConfigFactory.parse_file("resources/config.hocon")

db = pymongo.Connection(
        host=conf.get_string('db.mongo.host'),
        port=conf.get_int('db.mongo.port'),
    )[conf.get_string('db.mongo.db')]
# db.authenticate(
#     conf.get_string('db.mongo.user'),
#     conf.get_string('db.mongo.pw'),
# )
w_col = db["w"]
w_col.ensure_index([("type_key", pymongo.ASCENDING), ("date", pymongo.ASCENDING), ("hour", pymongo.ASCENDING)])

dir = sys.argv[1]
if dir[:-1] != "/":
    dir += '/'
file_name_list = os.listdir(dir)


def make_value(line, type_key):
    tks = line.split("\t")
    lat = float(tks[0])
    long = float(tks[1])
    if type_key == "wind":
        v = map(float, tks[2:])
    elif type_key == "weather":
        v = tks[2]
    else:
        v = float(tks[2])
    return {
        "lat": lat,
        "long": long,
        "value": v
    }

for each in file_name_list:
    file_name = dir + each
    print file_name
    each = each.split('.')[0]
    date = each[0:4]
    hour = each[4:6]
    type_key = each[7:]

    f = open(file_name, "r")
    values = [ make_value(line, type_key) for line in f.readlines() if not line ]
    f.close()
    if not values:
        print "alert"
        continue

    data_key = {
        "type_key": type_key,
        "date": date,
        "hour": hour
    }
    w_col.update(
        data_key,
        { "$set": { "value": values }},
        upsert=True
    )
