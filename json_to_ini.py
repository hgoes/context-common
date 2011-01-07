import rule
import json
import sys

with open(sys.argv[1]) as h:
    j = json.load(h)

r = rule.ClassifierSet.from_json(j)
r.to_ini(sys.argv[2])
