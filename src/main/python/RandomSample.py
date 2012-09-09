#!/usr/bin/env python
import sys,random

for line in sys.stdin:
	if (random.randint(1,100) <= int(sys.argv[1])):
		print line.strip()

