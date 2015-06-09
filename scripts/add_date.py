__author__ = 'badpoet'

import sys
sys.path.append(".")

import wlib.clients
import wlib.clients.mongo

conf = wlib.clients.get_conf()
db = wlib.clients.mongo.connection(conf)

for each in db["raw"].find():
    mongo_id = each["_id"]
    date = each["timestamp"][:8]
    db["raw"].update({"_id": mongo_id}, {"$set": {"date": date}})

