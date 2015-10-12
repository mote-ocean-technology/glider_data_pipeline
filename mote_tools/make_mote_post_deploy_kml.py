#!/bin/env python
#########################EDIT THESE SETTINGS TO FIT YOUR LOCATION#############
import operator
from heapq import nlargest
import os
import sys
import glob
import datetime
import time
from time import strftime, localtime
import string
import re
from sqlite3 import dbapi2 as sqlite
import telnetlib
import matplotlib
from matplotlib.pylab import *
import matplotlib.dates as mdates

if len(sys.argv) != 5:
    print "Got %d args" % (len(sys.argv))
    print "Usage: postDeployGT  glidername dataDir gliderColor DEBUG_LEVEL"
    sys.exit(1)
else:
    vehicleName=sys.argv[1]
    gliderColor = sys.argv[3]
    DEBUG = int(sys.argv[4])
#GLOBALS
gliderDataDir = sys.argv[2]
gliderLogDir = "%s/ascii_files/logs" % gliderDataDir
gliderSbdDir = "%s/binary_files/sbd" % gliderDataDir
gliderTbdDir = "%s/binary_files/tbd" % gliderDataDir


dinkumToolsDir = "/opt/dinkum"
#tbdSensors = 'sci_water_temp','sci_water_cond','sci_moteopd_volt'
tbdSensors = 'sci_water_temp','sci_water_cond'
sbdSensors = 'm_water_depth',''
speciesDict = {
'sci_moteopd_corr0':'Chlorophyte - Dunaliella',
'sci_moteopd_corr1':'Prymnesiophyte - Emiliania',
'sci_moteopd_corr2':'Cryptophyte - Hemiselmis',
'sci_moteopd_corr3':'Raphidophyte - Heterosigma',
'sci_moteopd_corr4':'Dinophyte - Karenia b.',
'sci_moteopd_corr5':'Dinophyte - Karenia m.',
'sci_moteopd_corr6':'Bacillariophyte - Pseudo-nitzschia del.',
'sci_moteopd_corr7':'Dinophyte - Prorocentrum',
'sci_moteopd_corr8':'Bacillariophyte - Thalassiosira p.',
'sci_moteopd_corr9':'Bacillariophyte - Thalassiosira w.',
'sci_moteopd_corr10':'Prasinophyte - Tetraselmis',
'sci_moteopd_corr11':'Cyanophyte - Trichodesmium'
}
#CONFIG SETTINGS
useSurface = 1
useSBD = 0
useTBD = 0
useGenus = 0


def createDB():
    global gliderlog
    global vehicleName
    haveSqliteDB = 0

    gliderLogFile = "%s.db" % vehicleName
    if (os.path.isfile(gliderLogFile)):
        haveSqliteDB = 1
        #gliderLogFile.db already exists so just connect and move on...
        gliderlog = sqlite.connect(gliderLogFile, isolation_level=None)
        gliderlog.text_factory = str
    else:
        haveSqliteDB = 0
        gliderlog = sqlite.connect(gliderLogFile, isolation_level=None)
        gliderlog.text_factory = str
        if (DEBUG > 0):
            print "CreateDB(): Creating %s" % gliderLogFile

    if (haveSqliteDB == 0):
        cur = gliderlog.cursor()
        cur.execute('create table surfaceReports (id integer primary key, \
vehicleName text, missionName text, missionNum text, currTime float, \
lon float, lat float, becauseWhy text, bearing float, \
distNextWaypoint int)')
        cur.execute('create table surfaceLogFiles (id integer primary key, \
logfile text,parsed boolean)')
        cur.execute('create table tbdFiles (id integer primary key,  \
tbdfile text,parsed boolean)')
        cur.execute('create table sbdFiles (id integer primary key,  \
sbdfile text,parsed boolean)')
        cur.execute('create table surfaceSensors (id integer primary key,\
currTime float, sensor text, sensorReading float)')
        cur.execute('create table tbdSensorReadings (id integer primary key, \
m_present_time float, sensor text, reading float)')
        cur.execute('create table sbdSensorReadings (id integer primary key, \
m_present_time float, sensor text, reading float)')
        cur.execute('create table surfaceDialog (id integer primary key, \
vehicle text, m_present_time float, logfile text, line text)')
        cur.close()


def writeHeaderKML():
    cursor = gliderlog.cursor()
    cursor.execute("SELECT reading from sbdSensorReadings where sensor = 'm_lat' ORDER BY id ASC limit 1")
    for (lat) in cursor:
        myLat = lat[0]
    cursor.execute("SELECT reading from sbdSensorReadings where sensor = 'm_lon' ORDER BY id ASC limit 1")
    for (lon) in cursor:
        myLon = lon[0]
    lon,lat = dinkumConvert(myLon,myLat)
    SELECT = "select date(strftime(\"%Y-%m-%d\",m_present_time,\
    'unixepoch'),'localtime') as date from sbdSensorReadings \
ORDER BY date DESC LIMIT 1"
    cursor.execute(SELECT)
    for (date) in cursor:
        myDate = date[0]

    cursor.close()
    print "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    print "<kml xmlns=\"http://earth.google.com/kml/2.0\">"
    print "<Folder>"
    print "<name>%s_%s</name>" % (vehicleName,myDate)
    print " <LookAt>"
    print "     <longitude>%0.4f</longitude>" % lon
    print "     <latitude>%0.4f</latitude>" % lat
    print "     <range>300000</range>"
    print "     <tilt>020</tilt>"
    print "     <heading>0</heading>"
    print " </LookAt>"



def writeTbdKML():
    if (DEBUG > 0):
        print "writeTbdKML()"
    curSBD = gliderlog.cursor()
    curSBD.execute("SELECT m_present_time, reading from sbdSensorReadings \
     WHERE sensor = 'm_lat' order by m_present_time DESC LIMIT 1")
    for (m_present_time, reading) in curSBD:
        m_lat = reading
    curSBD.execute("SELECT m_present_time, reading from sbdSensorReadings \
    WHERE sensor = 'm_lon' order by m_present_time DESC LIMIT 1")
    for (m_present_time, reading) in curSBD:
        m_lon = reading
    lon,lat = dinkumConvert(m_lon, m_lat)
    print "    <Folder>"
    print "   <name>DR Track</name>"
    print "<visibility>0</visibility>"
    #Build glider track here
    #
    print "   <Placemark>"
    print "<visibility>0</visibility>"
    print "       <name>DR Track</name>"
    print "       <LookAt>"
    print "          <longitude>%0.4f</longitude>" % (lon)
    print "          <latitude>%0.4f</latitude>" % (lat)
    print "          <range>25000</range>"
    print "          <tilt>20</tilt>"
    print "          <heading>90</heading>"
    print "       </LookAt>"
    print "       <Style>"
    print "          <LineStyle>"
    print "             <color>ffff0000</color>"
    print "             <width>2</width>"
    print "          </LineStyle>"
    print "       </Style>"
    print "       <LineString>"
    print "          <altitudeMode>absolute</altitudeMode>"
    print "          <coordinates>"
    curSBD.execute("SELECT m_present_time, sensor, reading from \
    sbdSensorReadings where sensor = 'm_lat' or sensor = 'm_lon' \
    ORDER BY m_present_time ASC")
    #Messy but unavoidable given current tbdSensorReadings structure
    #We get only one of m_lat or m_lon before calling dinkumConvert
    #for the very first row of the query. By using haveLat and haveLon
    #we make *certain* that we've gotten a good matching pair before
    #calling dinkumConvert and writing out to file.
    haveLat = 0
    haveLon = 0
    for (m_present_time, sensor, reading) in curSBD:
        if sensor == "m_lat":
            m_lat = float(reading)
            haveLat = 1
        if sensor == "m_lon":
            m_lon = float(reading)
            haveLon = 1
        if haveLat > 0:
            if haveLon > 0:
                lon,lat = dinkumConvert(m_lon,m_lat)
                print "          %0.4f,%0.4f,0" % (lon,lat)
                #ok, we're done. Reset haveLat and haveLon.
                haveLat = 0
                haveLon = 0
    print "          </coordinates>"
    print "      </LineString>"
    print "   </Placemark>"
    print "</Folder>"
    curSBD.close()


def writeFooterKML():
    print "</Folder>"
    print "</kml>"


def parseTbd():
 #2007-03-16 rdc
    #Figured out how to not have tempfile by using myBuffer
    #and os.popen3
    cur = gliderlog.cursor()
    curParsed = gliderlog.cursor()
    #
    #Regex for lines beginning with m_present_time
    #m_present_time structure: '1277271054.36734
    pattMPresentTime = re.compile('1[0-9]{9}\.[0-9]{5}')
    #
    #use this to tell if we've already figured out
    #table structure. Is this faster than exists
    #table in sqlite? profile to test...
    os.chdir(gliderTbdDir)
    SELECT = "SELECT tbdfile from tbdFiles where parsed = 0 ORDER BY \
    tbdfile ASC"
    cur.execute(SELECT)
    for(tbdfile) in cur:
        if DEBUG > 0:
            print "Running dbd2asc on %s" % tbdfile
        command = "/opt/dinkum/dbd2asc %s" % tbdfile
        (o,i,e) = os.popen3(command)
        myBuffer = i.xreadlines()
        cur = gliderlog.cursor()
        cur.execute('begin')
        for (line) in myBuffer:
            #2011-06-02 rdc changed to sci_ to reflect new tbd file format
            #Look to see if line starts with "sci_"  -- assume that a list
            #of sensors follows. Split into sensors, create tbdSensors
            #table and store values in tbdSensorReadings.
            if line.startswith("sci_"):
                #we need to split and create here, not print
                sensorList = line.split()
                if (DEBUG > 1):
                    print "TBD sensorList: %s" %(sensorList)
                if DEBUG > 1:
                    for (sensor) in sensorList:
                        print "Factored %s" % (sensor)
            if pattMPresentTime.search(line):
                index = 0
                matchobj = pattMPresentTime.search(line)
                if matchobj:
                    if DEBUG > 1:
                        print "MATCH ON pattMPresentTime"
                        print "m_present_time: %s" % matchobj.group(0)
                    m_present_time = float(matchobj.group(0))
                    sensorReadings = line.split()
                    if DEBUG > 1:
                        print sensorReadings
                    for (sensor) in sensorList:
                        #now insert into tbdSensorReadings
                        if sensorReadings[index] != "NaN":
                            reading = float(sensorReadings[index])
                            if DEBUG > 1:
                                print "Inserting %s with %0.4f" % (sensor, \
                                reading)
                            cur.execute("INSERT into tbdSensorReadings \
                            (m_present_time, sensor, reading) VALUES(?,?,?)",\
                            (m_present_time, sensor, reading))
                        index = index + 1
        cur.execute('commit')
        cur.close()
        SELECT = "UPDATE tbdFiles set parsed = '1' where tbdfile = '%s'" % \
        tbdfile
        curParsed.execute(SELECT)
    curParsed.close()

def parseSbd():
 #2007-03-16 rdc
    #Figured out how to not have tempfile by using myBuffer
    #and os.popen3
    cur = gliderlog.cursor()
    curParsed = gliderlog.cursor()
    #
    #Regex for lines beginning with m_present_time
    #m_present_time structure: '1277271054.36734
    pattMPresentTime = re.compile('1[0-9]{9}\.[0-9]{5}')
    #
    #use this to tell if we've already figured out
    #table structure. Is this faster than exists
    #table in sqlite? profile to test...
    os.chdir(gliderSbdDir)
    SELECT = "SELECT sbdfile from sbdFiles where parsed = 0 ORDER BY \
    sbdfile ASC"
    cur.execute(SELECT)
    for(sbdfile) in cur:
        if DEBUG > 0:
            print "Running dbd2asc on %s" % sbdfile
        command = "/opt/dinkum/dbd2asc %s" % sbdfile
        (o,i,e) = os.popen3(command)
        myBuffer = i.xreadlines()
        cur = gliderlog.cursor()
        cur.execute('begin')
        for (line) in myBuffer:
            #2011-06-02 rdc changed to sci_ to reflect new tbd file format
            #Look to see if line starts with "sci_"  -- assume that a list
            #of sensors follows. Split into sensors, create tbdSensors
            #table and store values in tbdSensorReadings.
            if line.startswith("c_"):
                #we need to split and create here, not print
                sensorList = line.split()
                if (DEBUG > 1):
                    print "SBD sensorList: %s" %(sensorList)
                if DEBUG > 1:
                    for (sensor) in sensorList:
                        print "Factored %s" % (sensor)
            if pattMPresentTime.search(line):
                index = 0
                matchobj = pattMPresentTime.search(line)
                if matchobj:
                    if DEBUG > 1:
                        print "MATCH ON pattMPresentTime"
                        print "m_present_time: %s" % matchobj.group(0)
                    m_present_time = float(matchobj.group(0))
                    sensorReadings = line.split()
                    if DEBUG > 1:
                        print sensorReadings
                    for (sensor) in sensorList:
                        #now insert into tbdSensorReadings
                        if sensorReadings[index] != "NaN":
                            reading = float(sensorReadings[index])
                            if DEBUG > 1:
                                print "Inserting %s with %0.4f" % (sensor, \
                                reading)
                            cur.execute("INSERT into sbdSensorReadings \
                            (m_present_time, sensor, reading) VALUES(?,?,?)",\
                            (m_present_time, sensor, reading))
                        index = index + 1
        cur.execute('commit')
        cur.close()
        SELECT = "UPDATE sbdFiles set parsed = '1' where sbdfile = '%s'" % \
        sbdfile
        curParsed.execute(SELECT)
    curParsed.close()


def dinkumConvert(gpsLon,gpsLat):
    gpsLon = float(gpsLon)
    gpsLat = float(gpsLat)
    latInt = int((gpsLat / 100.0))
    lonInt = int((gpsLon / 100.0))
    lat = (latInt + (gpsLat - (latInt * 100)) / 60.0)
    lon = (lonInt + (gpsLon - (lonInt * 100)) / 60.0)
    lat = eval("%0.4f" % (lat))
    lon = eval("%0.4f" % (lon))
    return lon,lat
    if DEBUG > 1:
        print "Converted %0.4f %0.4f to %0.4f %0.4f" % (gpsLon,gpsLat,lon,lat)

def parseLogFiles():
#2012-09-03 rdc@mote.org
#Totally revamped parseLog code. Now we DO NOT CARE what each line say...
#we just insert into surfaceDialog and keep trucking. Wwhen it comes time
#to build surface KML we THEN extract pertinent data from surfaceDialog using
#the regex matches for time, sensor, vehicle, etc. We can thus skip ALL the
#crap in surface dialogues but we'll now have it all in the DB for historical
#searching. And no matter what the glider says/does, we can extract the last
#known good position and sensor readings, even if they are partly garbled.
    cur = gliderlog.cursor()
    cur2 = gliderlog.cursor()
    cur3 = gliderlog.cursor()
    #
    SELECT = " select logfile from surfaceLogFiles where parsed = 0 ORDER BY \
    logfile asc"
    cur.execute(SELECT)
    for(logfile) in cur:
        if DEBUG > 0:
            print ("Parsing: " + '%s' % logfile)
        file = open('%s' % (logfile))
        cur3.execute('begin')
        for line in file.readlines():
            cur3.execute("INSERT into surfaceDialog(vehicle,logfile,line) \
            VALUES (?,?,?)", (vehicleName,logfile[0],line.strip() ))
            if DEBUG > 1:
                print line,
        cur3.execute('commit')
        SELECT2 = "UPDATE surfaceLogFiles set parsed = '1' where logfile = \
        '%s'" % logfile
        cur2.execute(SELECT2)
    cur.close()
    cur2.close()

def listLogFiles(gliderLogDir, fileType):
#2007-08-27 rdc
#Need to change this function so that it only
#reports NEW files, NOT files in the tbdFiles or
#surfaceLogFiles tables.  We are now always using
#a true file and not doing in-memory, so that the DB
#can be used by pyGV. This keeps me from processing
#log and tbd files twice per run.
#
#
    cur = gliderlog.cursor()
    fileCur = gliderlog.cursor()
    os.chdir(gliderLogDir)
    fileNames = glob.glob("*." + fileType)
    fileCount = len(fileNames)
    fileNumber = 0
    if(DEBUG > 0):
        print "listLogFile() found %d files" % fileCount
        print "listLogFiles(%s,%s)" % (gliderLogDir,fileType)

    for file in fileNames:
        if(DEBUG > 0):
            print "File %d %s" % (fileNumber,file)
            fileNumber+=1
        haveFile = 0
        if fileType == "log":
            SELECT = "SELECT logfile from surfaceLogFiles where logfile = \
            '%s'" % file
            fileCur.execute(SELECT)
            for(logfile) in fileCur:
                if(logfile !=  ""):
                    if(DEBUG > 0):
                        print "%s already inserted..." % file
                    haveFile = 1
            if(haveFile == 0):
                if(DEBUG > 0):
                    print "Inserting " + file + " into surfaceLogFiles"
                cur.execute("INSERT into surfaceLogFiles(logfile,parsed) \
                VALUES (?,?)", (file,0 ))

        if fileType == "tbd":
            SELECT = "SELECT tbdfile from tbdFiles where tbdfile = '%s'" % file
            fileCur.execute(SELECT)
            for(tbdfile) in fileCur:
                if(tbdfile !=  ""):
                    if(DEBUG > 0):
                        print "%s already dinkumProcessed" % file
                    haveFile = 1

            if(haveFile == 0):
                if (fileType == "tbd"):
                    if(DEBUG > 0):
                        print "Inserting " + file + " into tbdFiles"
                    cur.execute("INSERT into tbdFiles(tbdfile,parsed) VALUES \
                    (?,?)", (file,0 ))


        if fileType == "sbd":
            SELECT = "SELECT sbdfile from sbdFiles where sbdfile = '%s'" % file
            fileCur.execute(SELECT)
            for(sbdfile) in fileCur:
                if(sbdfile !=  ""):
                    if(DEBUG > 0):
                        print "%s already dinkumProcessed" % file
                    haveFile = 1

            if(haveFile == 0):
                if (fileType == "sbd"):
                    if(DEBUG > 0):
                        print "Inserting " + file + " into sbdFiles"
                    cur.execute("INSERT into sbdFiles(sbdfile,parsed) VALUES \
                    (?,?)", (file,0 ))

    cur.close()
    return (fileCount)

def genSurfaceReports(vehicleName,numSurfSensors):
    missionNum = 0
    if DEBUG > 0:
        print "genSurfaceReports()"
    curQuery = gliderlog.cursor()
    curInsert = gliderlog.cursor()
    curGPS = gliderlog.cursor()
    curMission = gliderlog.cursor()
    curBecause = gliderlog.cursor()
    curWaypoint = gliderlog.cursor()
    curSurfSensor = gliderlog.cursor()
    curSurfInsert = gliderlog.cursor()

    #Always use CURR TIME, GPS LOC and SURF SENSORS
    curInsert.execute('begin')
    select = "select rowid,line from surfaceDialog where line LIKE \
    '%Curr Time%'"
    curQuery.execute(select)
    for(rowid,currTimeLine) in curQuery:
        #CURR TIME
        pattCurrTime = re.compile('(^Curr Time: )([A-Za-za-z].*201[0-9]+).*(MT.*[0-9]+)')
        matchobj = pattCurrTime.match(currTimeLine)
        if matchobj:
            currTime = time.mktime(time.strptime(matchobj.group(2)))
        #GPS LOC
        pattGPS = re.compile('(^GPS Location:\s+)([0-9]+\.[0-9]+).*(-[0-9]+\.[0-9]+)')
        selectGPS = "select line from surfaceDialog where line like \
        'GPS Location%%' AND rowid > %d LIMIT 1" % rowid
        curGPS.execute(selectGPS)
        for(gpsLine) in curGPS:
            matchobj = pattGPS.match(gpsLine[0])
            if matchobj:
                gpsLat = matchobj.group(2)
                gpsLon = matchobj.group(3)
                lon,lat = dinkumConvert(gpsLon,gpsLat)
        #SURFACE SENSORS
        pattSurfSensor = re.compile('(sensor:)([m_a-zA-Z0-9]+)(\([a-zA-Z]+\)=(-?[0-9]*\.?[0-9]*))')
        selectSurfSensor = "select line from surfaceDialog where line like \
'sensor:%%' and ROWID > %d ORDER BY ROWID ASC LIMIT %d" % \
(rowid,numSurfSensors)
        curSurfSensor.execute(selectSurfSensor)
        for(surfSensorLine) in curSurfSensor:
            #to handle water_vx(m/s) and water_vy(m/s)
            surfSensorLine = surfSensorLine[0]
            surfSensorLine = string.replace(surfSensorLine,'/','')
            matchobj = pattSurfSensor.match(surfSensorLine)
            if matchobj:
                sensor = (matchobj.group(2))
                sensor = sensor.strip()
                sensorReading = eval(matchobj.group(4))
                sensorReading = eval("%0.2f" % sensorReading)
                curSurfInsert.execute("INSERT into surfaceSensors \
                (currTime,sensor,sensorReading) VALUES (?,?,?)",\
                (currTime,sensor,sensorReading))

        #MISSION NAME
        pattMissionName = re.compile('(^MissionName:([A-Za-z0-9]+))')
        selectMission = "select line from surfaceDialog where \
line like 'MissionName%%' AND rowid > %d LIMIT 1" % rowid
        curMission.execute(selectMission)
        for(missionLine) in curMission:
            matchobj = pattMissionName.match(missionLine[0])
            if matchobj:
                missionName = matchobj.group(2)
        #BECAUSE_WHY
        pattBecauseWhy = re.compile('(^Because:([a-zA-Z]+.*)(\[))')
        selectBecause = "select line from surfaceDialog where line \
like 'Because%%' AND ROWID > %d LIMIT 1" % rowid
        curBecause.execute(selectBecause)
        for(becauseLine) in curBecause:
            matchobj = pattBecauseWhy.match(becauseLine[0])
            becauseWhy = matchobj.group(2)
        #NEXT WAYPOINT
        pattWaypoint = re.compile('(^Waypoint: ).*(Range: )([0-9]+).*(Bearing:\s+)([0-9]+)')
        selectWaypoint = "select line from surfaceDialog where \
line like 'Waypoint%%' AND ROWID > %d LIMIT 1" % rowid
        curWaypoint.execute(selectWaypoint)
        for(wayPointLine) in curWaypoint:
            matchobj = pattWaypoint.match(wayPointLine[0])
            if matchobj:
                distNextWaypoint = int(matchobj.group(3))
                bearing = int(matchobj.group(5))

        #Write completed in-mission surfRep into surfaceReports
        try:
            curInsert.execute("INSERT into surfaceReports \
            (vehicleName,missionName,missionNum,currTime,lon,lat,\
            becauseWhy,bearing,distNextWaypoint) VALUES (?,?,?,?,?,?,?,?,?)",\
            (vehicleName,missionName,missionNum,currTime,lon,lat,becauseWhy,\
            bearing,distNextWaypoint))
        except:
            if DEBUG > 0:
                print "Incomplete Surface Report!"
    curInsert.execute('commit')
    curQuery.close()
    curInsert.close()
    curGPS.close()
    curMission.close()
    curBecause.close()
    curWaypoint.close()
    curSurfInsert.close()

def writeSurfaceKML(numSurfSensors):
    surfacingCount = 0
    #First check surfaceReports to make sure we have data.
    #If not, we'll barf, so exit until we gets us some tasty bits.
    if DEBUG > 0:
        print "writeSurfaceKML(%d)" % numSurfSensors
    cur = gliderlog.cursor()
    SELECT = "select count(ROWID) from surfaceReports"
    cur.execute(SELECT)
    for (count) in cur:
        if(count[0] == 0):
            return
    cur.close()

    #First we build folders grouped by date
    #and then sort by time within each date folder
    cur = gliderlog.cursor()
    SELECT = "select distinct date(strftime(\"%Y-%m-%d %H:%M\",currTime,\
    'unixepoch'),'localtime') \
    as date from surfaceReports group by date ORDER BY DATE ASC"
    cur.execute(SELECT)
    print "<Folder>"
    print "<name>Surface Reports</name>"
    for (date) in cur:
        dateFormat = "%Y-%m-%d %H:%M:%S"
        timeFormat = "%Y-%m-%d %H:%M:%S"
        date = '%s' % date
        print "<Folder>"
        print "<name>%s</name>" % date
        SELECT2 = "select distinct date(strftime(\"%s\",currTime,'unixepoch'),\
        'localtime') \
        as date2, time(strftime(\"%s\",currTime,'unixepoch'),'localtime') as \
        time, currTime,becauseWhy,missionName,lon,lat,bearing, \
        distNextWaypoint from surfaceReports WHERE date2 = '%s' \
        group by lat,lon order by time ASC" % (dateFormat,timeFormat,date)
        cur2 = gliderlog.cursor()
        cur2.execute(SELECT2)
        for (date2,time,currTime,becauseWhy,missionName,lon,lat,bearing,\
        distNextWaypoint) in cur2:
            print "        <Placemark>"
            #We want this to be popup onmouseover, not displayed at all times
            if surfacingCount == 0:
                print "    <name>Deployment Location</name>"
            print "        <description>"
            print "            <![CDATA["
            print "            <h2><b><center>%s</center></b></h2><hr>" % \
            (vehicleName)
            print "             <table width=300>"
            print "            <tr><td><b>Date/Time:</td></b><td>%s %s</td>\
            </tr>" % (date2,time)
            print "            <tr><td><b>Position:</td></b><td>%0.4f N %0.4f \
            W</td></tr>" % (lat,lon)
            print "            <tr><td><b>Mission:</td></b><td>%s</td></tr>" \
            % (missionName)
            print "            <tr><td><b>Why Surface:</td></b><td>%s</td>\
            </tr>" % (becauseWhy)
            print "            </table>"
            print "            <hr>"
            print "            <h3><i>Surface Sensors:</h3></i>"
            cur3 = gliderlog.cursor()
            #2012-10-02 get surface sensors working in genSurfaceReports
            #now we report surface sensors in both historical and last pos.
            SELECT3 = "select sensor, sensorReading from surfaceSensors\
 where currTime > %d ORDER BY currTime ASC LIMIT %d" % (currTime, \
 numSurfSensors)
            cur3.execute(SELECT3)
            print "             <table width=300>"
            for (sensor, sensorReading) in cur3:
                print "            <tr><td><b>%s:</b></td><td>%0.2f</td>\
                </tr>" % (sensor,sensorReading)
            print "             </table>"
            cur3.close()

            print "             <hr>"

            print "            <h3><i>Next Waypoint:</h3></i>"
            print "            <table width=300>"
            print "            <tr><td><b>Range:</b></td><td> %d meters</td>\
            </tr>" % (distNextWaypoint)
            print "            <tr><td><b>Bearing:</b></td><td> %d degrees\
            </td></tr>" % (bearing)
            print "            </table>"
            print "            <hr/>"
            pyImage = "python-powered-w-70x28.png"
            print "<center><img src = 'http://www.python.org/static/community_logos/%s'></img>" % (pyImage)
            print "            </center>"

            print "            ]]>"
            print "        </description>"
            print "        <visibility>1</visibility>"
            print "        <Style>"
            print "        <LabelStyle>"
            print "            <color>ffffffff</color>"
            print "            <colorMode>normal</colorMode>"
            print "            <scale>1.5</scale>"
            print "        </LabelStyle>"
            print "            <BalloonStyle><text>$[description]</text>\
            </BalloonStyle>"

            if (surfacingCount == 0):
                print "            <IconStyle>"
                print "                <Icon>"
                print "                <href>http://maps.google.com/mapfiles/kml/paddle/ylw-diamond.png</href>"
                print "                </Icon>"
                print "            </IconStyle>"
                print "         </Style>"
            else:
                print "             <IconStyle>"
                print "                 <scale>.5</scale>"
                print "            <color>%s</color>" % (gliderColor)
                print "                <Icon>"
                print "                 <href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href>"
                print "                </Icon>"
                print "            </IconStyle>"
                print "        </Style>"
            print "        <Point>"
            print "            <coordinates>"
            print "            %0.4f,%0.4f,0" %(lon,lat)
            print "            </coordinates>"
            print "        </Point>"
            print "        </Placemark>"
            surfacingCount += 1
        cur2.close()
        print "</Folder>"
    cur.close()
    #Now we build the glider track in one folder by grubbing
    #all the surface points from surfaceLog
    print "<Placemark>"
    print "   <name>Surfacings Track</name>"
    print "   <LookAt>"

    print "   <longitude>%0.4f</longitude>" % lon
    print "   <latitude>%0.4f</latitude>" % lat
    print "   <range>25000</range>"
    print "   <tilt>20</tilt>"
    print "   <heading>90</heading>"
    print "   </LookAt>"
    print "   <visibility>1</visibility>"
    print "   <Style>"
    print "      <LineStyle>"
    print "      <color>%s</color>" % gliderColor
    print "      <width>2</width>"
    print "      <visibility>1</visibility>"
    print "      </LineStyle>"
    print "   </Style>"
    print "   <LineString>"
    print "   <altitudeMode>absolute</altitudeMode>"
    print "   <coordinates>"
    cur = gliderlog.cursor()
    SELECT = "select distinct lon,lat,currTime from surfaceReports order by \
    currTime"
    cur.execute(SELECT)
    try:
        for (lon,lat,currTime) in cur:
            print "      %0.4f,%0.4f,0" % (lon,lat)
    except:
        if DEBUG > 0:
            print "INCOMPLETE SURFACE REPORT -- MISSING currTime,lat or lon"
        lat = 0.00
        lon = 0.00
        time = 0

    print "   </coordinates>"
    print "   </LineString>"
    print "</Placemark>"
    #Last glider posit:
    print "        <Placemark>"
    print "        <name>Last Surfacing</name>"
    print "        <description>"
    print "            <![CDATA["
    print "            <h1><b><center>%s</center></b></h1><br>" % (vehicleName)
    print "            <table width='300'>"
    print "            <tr><td><b>Date/Time:</td></b><td>%s %s</td></tr>" % \
    (date2,time)
    print "            <tr><td><b>Position:</td></b><td>%0.4f N %0.4f W</td>\
    </tr>" % (lat,lon)
    print "            <tr><td><b>Mission:</b></td><td>%s</td></tr>" % \
    (missionName)
    print "            <tr><td><b>Why Surface:</td></b><td>%s</td></tr>" % \
    (becauseWhy)
    print "             </table>"
    print "            <hr>"
    print "            <h3><i>Surface Sensors:</h3></i>"
    cur3 = gliderlog.cursor()
    print "             <table width=300>"
    SELECT3 = "select sensor, sensorReading from \
    surfaceSensors ORDER BY currTime DESC LIMIT 6"
    cur3.execute(SELECT3)
    for (sensor, sensorReading) in cur3:
        print "            <tr><td><b>%s:</b></td><td>%0.2f</td></tr>" %\
        (sensor,sensorReading)
    print "             </table>"
    print "            <hr>"
    cur3.close()
    if useSBD:
        curTBD = gliderlog.cursor()
        #TBD SENSORS
        print "            <h3><i>SBD/TBD Sensors:</h3></i>"
        print "             <table width=300>"
        for tbdSensor in tbdSensors:
            SELECT_TBD = " select sensor,reading from tbdSensorReadings where\
            sensor = '%s' order by m_present_time desc limit 1;" % (tbdSensor)
            curTBD.execute(SELECT_TBD);
            for(sensor,reading) in curTBD:
                print "            <tr><td><b>%s:</b></td><td>%0.2f</td></tr>"\
                % (sensor,reading)
        curTBD.close()

        #SBD SENSORS
        curSBD = gliderlog.cursor()
        for sbdSensor in sbdSensors:
            SELECT_SBD = " select sensor,reading from sbdSensorReadings where\
            sensor = '%s' order by m_present_time desc limit 1;" % (sbdSensor)
            curSBD.execute(SELECT_SBD)
            for(sensor,reading) in curSBD:
                print "            <tr><td><b>%s:</b></td><td>%0.2f</td></tr>"\
                % (sensor,reading)
            print "         </table>"
        curSBD.close()
        print "<hr>"

    #2011-09-15 rdc@mote.org
    #Loop through speciesDict and get all sci_moteopd_corrN variables being recorded
    #Look up most recent value for all variables and choose the max.
    #Report the MAX VAR and display apprproate icon instead of green square
    #MULTI SPECIES

    #PUT THIS IN TABLE
    if useGenus == 1:
        classGenus = {}
        curCORR = gliderlog.cursor()
        for corr, species in speciesDict.iteritems():
            SELECT_CORR = "select sensor, reading from tbdSensorReadings where\
            sensor = '%s' AND reading != 0 order by m_present_time desc \
            limit 1;" % (corr)
            curCORR.execute(SELECT_CORR)
            for(sensor, reading) in curCORR:
                classGenus[species] = float(reading)
        curCORR.close()
        print "             <h3><i>Class-Genus Correlations:</i></h3>"

        count = 0
        for genus, value in nlargest(3,classGenus.iteritems(),\
        key=operator.itemgetter(1)):
            print "<b>%s:</b>   %0.2f<br>" % (genus, value)
        print "<hr>"
    print "            <h3><i>Next Waypoint:</h3></i>"
    print "            <table width=300>"
    print "            <tr><td><b>Range:</b></td><td> %d meters</td></tr>" % \
    (distNextWaypoint)
    print "            <tr><td><b>Bearing:</b></td><td> %d degrees</td></tr>" \
    % (bearing)
    print "            </table>"
    print "             <hr>"
    print "            <center>"
    pyImage = "python-powered-w-70x28.png"
    print "<center><img src = 'http://www.python.org/static/community_logos/%s'>" %\
    (pyImage)
    print "            </center>"
    print "            ]]>"
    print "        </description>"
    print "        <visibility>1</visibility>"
    print "        <Style>"
    print "        <LabelStyle>"
    print "            <color>ffffffff</color>"
    print "            <colorMode>normal</colorMode>"
    print "            <scale>1.5</scale>"
    print "        </LabelStyle>"
    print "            <BalloonStyle><text>$[description]</text>\
    </BalloonStyle>"
    print "            <IconStyle>"
    print "                 <color>%s</color>" % (gliderColor)
    print "                 <scale>1.5</scale>"
    print "                <heading>%d</heading>" % bearing
    print "                <Icon>"
    print "                     <href>http://maps.google.com/mapfiles/kml/shapes/airports.png</href>"
    print "                </Icon>"
    print "            </IconStyle>"
    print "        </Style>"
    print "        <Point>"
    print "            <coordinates>"
    print "            %0.4f,%0.4f,0" %(lon,lat)
    print "            </coordinates>"
    print "        </Point>"
    print "        </Placemark>"
    #Close Surface Reports folder
    print "</Folder>"
    cur.close()


def Main():
    #we'll get this from config file when we switch
    numSurfSensors = 4
    createDB()
    fileCount = 0
    #process SBD and TBD first so that we have sbd/tbd sensors
    #available for surf rep infobox
    fileCount = 0
    fileCount = listLogFiles(gliderSbdDir,"sbd")
    if fileCount > 0:
        parseSbd()

    fileCount = 0
    fileCount = listLogFiles(gliderTbdDir,"tbd")
    if fileCount > 0:
        parseTbd()

    fileCount = listLogFiles(gliderLogDir,"log")
    if fileCount > 0:
        parseLogFiles()
        genSurfaceReports(vehicleName,numSurfSensors)

    writeHeaderKML()
    writeSurfaceKML(numSurfSensors)
    writeTbdKML()
    writeFooterKML()
Main()
