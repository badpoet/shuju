__author__ = 'badpoet'

import json
import pymongo
import pyhocon
from flask import Flask, request

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


@app.route("/data/<q_type>/<q_date>/<q_hour>", methods=["GET"])
def query(q_type, q_date, q_hour):
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
    app.run(debug=True, port=9000)