__author__ = 'badpoet'

import codecs
import requests
from time import sleep

fin = codecs.open("city_codes.txt", "r", "utf8")
fout = codecs.open("city_codes_gib.txt", "w", "utf8")

pname = ""

def get_gi_via_baidu(p, d, c):

    params = {
        "q": c,
        "scope": 1,
        "output": "json",
        "ak": "xef2aaYbFbCWqekTlf6FNnjc",
        "region": p
    }
    resp = requests.get("http://api.map.baidu.com/place/v2/search", params=params)
    j = resp.json()
    t = j["results"][0]["location"]
    return (t["lat"], t["lng"])

for line in fin.readlines():
    cid, p, d, c = line.strip().split(" ")
    try:
        gi = get_gi_via_baidu(p, d, c)
        sleep(0.5)
    except Exception, e:
        fout.write(line.strip() + " not found\n")
        print p, d, c, e
    else:
        print p, d, c, gi
        fout.write(line.strip() + " " + str(gi[0]) + " " + str(gi[1]) + "\n")
fout.close()
