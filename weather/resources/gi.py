__author__ = 'badpoet'

import codecs
import re

fin = codecs.open("city_codes.txt", "r", "utf8")
fout = codecs.open("city_codes_gi.txt", "w", "utf8")
gin = codecs.open("GI-China.xml", "r", "utf8")
gi_dict = {}

pname = ""

for line in gin.readlines():
    t = re.findall(r'\<provinces name="(.*?)"\>', line)
    if len(t):
        pname = t[0]
        gi_dict[pname] = {}
    else:
        t = re.findall(r'<city name="(.*?)" longitude="(.*?)" latitude="(.*?)"', line)
        if len(t):
            cname, lo, la = t[0]
            gi_dict[pname][cname] = (float(lo), float(la))
gin.close()

for line in fin.readlines():
    cid, p, d, c = line.strip().split(" ")
    try:
        gi = gi_dict[p][c]
    except:
        fout.write(line.strip() + " not found\n")
    else:
        fout.write(line.strip() + " " + str(gi[0]) + " " + str(gi[1]) + "\n")
fout.close()
