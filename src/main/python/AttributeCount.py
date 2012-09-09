#!/usr/bin/env python
import sys
index = int(sys.argv[1])
for line in sys.stdin:
	fields = line.strip().split(",")
	print "LongValueSum:" + fields[index].strip() + "\t" + "1"
