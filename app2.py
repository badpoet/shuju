__author__ = 'badpoet'

import json
import pymongo
import pyhocon
from flask import Flask, request, abort

app = Flask(__name__)

# FIXME keep token in secret

conf = pyhocon.ConfigFactory.parse_file("resources/config.hocon")

TOKEN = conf.get_string('app.token')
db = pymongo.Connection(
        host=conf.get_string('db.mongo.host'),
        port=conf.get_int('db.mongo.port'),
    )[conf.get_string('db.mongo.db')]
# db.authenticate(
#     conf.get_string('db.mongo.user'),
#     conf.get_string('db.mongo.pw'),
# )
w_col = db["w"]
w2_col = db["w2"]
w_col.ensure_index([("type_key", 1), ("year", 1), ("date", 1), ("hour", 1)])
TIMESTAMP_ASCENDING = [("year", 1), ("date", 1), ("hour", 1)]
TIMESTAMP_DESCENDING = [("year", -1), ("date", -1), ("hour", -1)]
w_col.ensure_index(TIMESTAMP_ASCENDING)
w_col.ensure_index(TIMESTAMP_DESCENDING)
w2_col.ensure_index([("type_key", 1), ("timestamp", 1)])


@app.route("/timestamp", methods=["GET"])
def timestamp_range():
    token = request.args.get("token", "")
    if not token == TOKEN:
        return abort(403)
    a = w2_col.find_one(sort=[("timestamp", 1)])
    b = w2_col.find_one(sort=[("timestamp", -1)])
    if not a or not b:
        res = {
            "status": "none"
        }
        return json.dumps(res)
    a = a["timestamp"]
    b = b["timestamp"]
    res = {
        "status": "ok",
        "date_a": a[4:8],
        "hour_a": a[8:10],
        "year_a": a[0:4],
        "date_b": b[4:8],
        "hour_b": b[8:10],
        "year_b": b[0:4]
    }
    return json.dumps(res)


@app.route("/data/<q_type>/<q_year>/<q_date>/<q_hour>", methods=["GET"])
def query(q_type, q_year, q_date, q_hour):
    print "QUERY IN", q_type, q_year, q_date, q_hour
    token = request.args.get("token", "")
    if not token == TOKEN:
        res = {
            "status": "error",
            "err": "invalid token",
            "result": []
        }
        return json.dumps(res, ensure_ascii=False)
    timestamp = q_year + q_date + q_hour
    result = w2_col.find({
        "timestamp": timestamp,
        "type_key": q_type
    })
    values = [{"value": each["value"], "lat": each["lat"], "long": each["long"]} for each in result]
    if len(values) == 0:
        res = {
            "status": "none",
            "result": []
        }
        return json.dumps(res, ensure_ascii=False)
    res = {
        "status": "ok",
        "result": values
    }
    print "QUERY OUT", q_type, q_year, q_date, q_hour
    return json.dumps(res, ensure_ascii=False)

import sys

if __name__ == "__main__":
    port = int(sys.argv[1])
    app.run(host="0.0.0.0", port=port, debug=True)
