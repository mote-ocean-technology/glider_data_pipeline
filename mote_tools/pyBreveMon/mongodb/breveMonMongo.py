#!/usr/bin/env python
import pymongo

client = pymongo.MongoClient()
db = client['opd_mongo']
collection = db.opd_data

#Get serial, site and deployed from MySQL socool.sites
#Get data from mongodb.opd_mongo.opd_data
serialNumber = 24
for doc in db.opd_data.find({"SerialNumber" : serialNumber }).sort([("Date",pymongo.DESCENDING)]).limit(1):
    for key in doc.iterkeys():
        print key
    simIndex = doc['SI_DINOKARB']
    print "The current SimIndex is %0.2f" % simIndex
