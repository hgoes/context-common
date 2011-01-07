import rule
import json
import ConfigParser
import sys

p = ConfigParser.ConfigParser()
p.read(sys.argv[1])

r = rule.ClassifierSet.from_ini(p)
with open(sys.argv[2],'w') as h:
    json.dump(r.to_json(),h,indent=True)
