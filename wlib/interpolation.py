__author__ = 'badpoet'

import clients
import clients.mongo
from datetime import datetime, timedelta
from time import sleep

class Interpolator(object):

    def __init__(self, conf):
        db = clients.mongo.connection(conf)
        self.raw_col = db["raw"]
        self.w_col = db["w2"]
        self.w_col.ensure_index([("type_key", 1), ("timestamp", 1)])
        self.w_col.ensure_index([("type_key", 1), ("timestamp", -1)])
        self.gk_dict = clients.gk_dict
        self.DES = [("timestamp", -1)]
        self.ASC = [("timestamp", 1)]

    def interpolate(self, year, month, day, hour):
        timestamp = datetime(year, month, day, hour).strftime("%Y%m%d%H") + "00"
        cnt = 0
        cnt += self.interpolate_one("rain", timestamp)
        cnt += self.interpolate_one("temp", timestamp)
        cnt += self.interpolate_one("humid", timestamp)
        cnt += self.interpolate_one("aqi", timestamp)
        cnt += self.interpolate_one("wind", timestamp)
        return cnt

    def interpolate_data(self, ya, yb, xa, xb):
        return (ya * xb + yb * xa) / (xa + xb)

    def interpolate_one(self, type_key, timestamp):
        cnt = 0
        for gk in self.gk_dict.values():
            lat, long = map(float, gk.split("+"))
            print {"type_key": type_key, "lat": lat, "long": long, "timestamp": {"$lt", timestamp}}
            left = self.raw_col.find_one(
                {"type_key": type_key, "lat": lat, "long": long, "timestamp": {"$lt", timestamp}})
            right = self.raw_col.find_one(
                {"type_key": type_key, "lat": lat, "long": long, "timestamp": {"$gt", timestamp}})
            if not left or not right:
                continue
            lv = left["value"]
            rv = right["value"]
            base = clients.stamp_to_obj(timestamp)
            lt = (clients.stamp_to_obj(left["timestamp"]) - base).seconds
            rt = (clients.stamp_to_obj(right["timestamp"]) - base).seconds
            if type_key == "wind":
                v = (self.interpolate_data(lv[0], rv[0], lt, rt), self.interpolate_data(lv[1], rv[1], lt, rt))
            else:
                v = self.interpolate_data(lv, rv, lt, rt)
            self.w_col.update({
                "timestamp": timestamp[:10],
                "type_key": type_key,
                "lat": lat,
                "long": long
            }, {
                "$set": {"value": v}
            }, upsert=True)
            cnt += 1
        return cnt


if __name__ == "__main__":
    conf = clients.get_conf()
    interpolator = Interpolator(conf)
    base = datetime(2015, 5, 11, 12)
    while True:
        print "Interpolate", base.strftime("%Y%m%d%H")
        interpolator.interpolate(base.year, base.month, base.day, base.hour)
        base += timedelta(hours=1)
        if base > datetime.now() - timedelta(hours=1.5):
            sleep(3600)