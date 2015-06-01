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
w_col.ensure_index([("type_key", 1), ("year", 1), ("date", 1), ("hour", 1)])
TIMESTAMP_ASCENDING = [("year", 1), ("date", 1), ("hour", 1)]
TIMESTAMP_DESCENDING = [("year", -1), ("date", -1), ("hour", -1)]
w_col.ensure_index(TIMESTAMP_ASCENDING)
w_col.ensure_index(TIMESTAMP_DESCENDING)


@app.route("/timestamp", methods=["GET"])
def timestamp_range():
    token = request.args.get("token", "")
    if not token == TOKEN:
        return abort(403)
    a = w_col.find_one(sort=TIMESTAMP_ASCENDING)
    b = w_col.find_one(sort=TIMESTAMP_DESCENDING)
    if not a or not b:
        res = {
            "status": "none"
        }
        return json.dumps(res)
    res = {
        "status": "ok",
        "date_a": a["date"],
        "hour_a": a["hour"],
        "year_a": a["year"],
        "date_b": b["date"],
        "hour_b": b["hour"],
        "year_b": b["year"]
    }
    return json.dumps(res)


@app.route("/data/<q_type>/<q_year>/<q_date>/<q_hour>", methods=["GET"])
def query(q_type, q_year, q_date, q_hour):
    token = request.args.get("token", "")
    if not token == TOKEN:
        res = {
            "status": "error",
            "err": "invalid token",
            "result": []
        }
        return json.dumps(res, ensure_ascii=False)
    result = w_col.find_one({
        "type_key": q_type,
        "year": q_year,
        "date": q_date,
        "hour": q_hour
    })
    if result == None or "value" not in result or not result["value"]:
        res = {
            "status": "none",
            "result": []
        }
        return json.dumps(res, ensure_ascii=False)
    res = {
        "status": "ok",
        "result": result["value"]
    }
    return json.dumps(res, ensure_ascii=False)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=True)
