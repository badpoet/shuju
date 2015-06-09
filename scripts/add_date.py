__author__ = 'badpoet'

import sys
sys.path.append(".")

import wlib.clients
import wlib.clients.mongo

conf = wlib.clients.get_conf()
db = wlib.clients.mongo.connection(conf)

date_bak = ""
for each in db["raw"].find().sort([("timestamp", 1)]):
    mongo_id = each["_id"]
    date = each["timestamp"][:8]
    if not date == date_bak:
        print date
        date_bak = date
    db["raw"].update({"_id": mongo_id}, {"$set": {"date": date}})

print "done"
