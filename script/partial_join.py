#!/usr/bin/python
#coding:utf-8

import os, sys, getopt
import traceback
from hebe_pb2 import DataPartial, Transform, Format, Settings
from google import protobuf
from google.protobuf import text_format
import re

settings = Settings()
try:
	fp = open(sys.argv[1],"rb")
	wire_text = fp.read()
	text_format.Merge(wire_text, settings)
	fp.close()
except:
	print traceback.format_exc()
	sys.exit(255)


res = "insert overwrite directory '"+settings.hdfs_path+"/"+settings.name+"/"+settings.partial[0].name+"' select "
columnsList = settings.partial[0].columns.split(",")
typeList = settings.partial[0].type.split(",")

count = 0
for column in columnsList:
	if typeList[count] == 's':
		res += (column + ",")
	elif typeList[count] == 'f':
		res += "floor(cast("+column+" as float)*"+settings.partial[0].convert+") as "+column+"," 
	else:
		print "type error"
	count += 1
res = res[:-1]
if settings.partial[0].dtfrom:
	res += (" from " +settings.partial[0].table + " where dt>=" + settings.partial[0].dtfrom)
else:
	res += (" from " +settings.partial[0].table)
print res

	
