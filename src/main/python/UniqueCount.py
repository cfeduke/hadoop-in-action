#!/usr/bin/env python

import sys

index1 = int(sys.argv[1])
index2 = int(sys.argv[2])
for line in sys.stdin:
  fields = line.split(",")
  print "UniqValueCount:" + fields[index1] + "\t" + fields[index2]
