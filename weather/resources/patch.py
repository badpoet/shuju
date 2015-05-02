import codecs
f = codecs.open("city_codes_gib_bak.txt", "r", "utf8")
g = codecs.open("city_codes_gib_patched.txt", "w", "utf8")
h = codecs.open("patch.txt", "r", "utf8")
h_dict = {}
for line in h.readlines():
    if not line.strip(): continue
    cid = line.strip().split(" ")[0]
    loc = line.strip().split(" ")[-1]
    print loc
    h_dict[cid] = loc.split(",")[1] + " " + loc.split(",")[0]

for line in f.readlines():
    line = line.strip()
    if not line: continue
    tmp = line.split(" ")
    cid = tmp[0]
    s = " ".join(tmp[0:4])
    if cid in h_dict:
        g.write(s + " " + h_dict[cid] + "\n")
    else:
        g.write(line + "\n")

g.close()
