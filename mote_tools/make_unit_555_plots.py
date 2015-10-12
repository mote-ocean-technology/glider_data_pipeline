#!/usr/bin/env python
"""
    Title:      make_unit_555_plots
    Date:       2012-11-27
    Author:     rdc@mote.org
    Modified:   2015-09-10
        By:     codemonkey@gcoos.org
    Inputs:     log and binary files
    Outputs:    scatter plots
    pylint:     9.10
"""
#imports
import os
import glob
import re
from sqlite3 import dbapi2 as sqlite
import matplotlib.pyplot as plt
import seawater.csiro as sw
import seawater.gibbs as gsw
from matplotlib import dates as mpd
from matplotlib import cm as cm
from matplotlib import colors as colors
import numpy as np
import sys
import time
import subprocess
import csv
import MySQLdb

logoFile = '/opt/glider_data_pipeline/mote_tools/mote_logo.png'

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

def createDB():
    """
    Module:     createDB()
    Date:       2012-11-27
    Author:     rdc@mote.org
    Modified:   2013-10-01
        By:     rdc@mote.org
    Inputs:     None
    Outputs:    SQLite database file
    Purpose:    Creates SQLite database file to hold values for plot generation.
                We populate with a limited set of glider sensors which can be
                modified at any time by changing the schema.
    """

    global glider_name
    global gliderlog
    haveSqliteDB = 0


    gliderLogFile = "./%s-plots-sqlite.db" % glider_name
    if (os.path.isfile(gliderLogFile)):
        haveSqliteDB = 1
        #gliderLogFile.db already exists so just connect and move on...
        gliderlog = sqlite.connect(gliderLogFile, isolation_level=None)
        gliderlog.text_factory = str
    else:
        print "Creating database..."
        haveSqliteDB = 0
        gliderlog = sqlite.connect(gliderLogFile, isolation_level=None)
        gliderlog.text_factory = str
        if (DEBUG > 0):
            print "CreateDB(): Creating %s" % gliderLogFile
    if (haveSqliteDB == 0):
        cur = gliderlog.cursor()
        cur.execute('create table tbdFiles (id integer primary key,  \
tbdfile text,parsed boolean)')
        cur.execute('create table sbdFiles (id integer primary key,  \
sbdfile text,parsed boolean)')
        cur.execute('create table dbdFiles (id integer primary key,  \
dbdfile text,parsed boolean)')
        cur.execute('create table ebdFiles (id integer primary key,  \
ebdfile text,parsed boolean)')
        cur.execute('create table plotValues (id integer primary key, \
m_present_time float, m_depth float, m_water_depth float, sci_water_temp float,\
sci_water_cond float, sci_water_pressure float, calc_salinity float,\
absSalinty float, calc_density float, sci_bbfl2s_chlor_scaled float, \
sci_moteopd_corr1 float, sci_moteopd_corr2 float, sci_moteopd_corr3 float,\
sci_moteopd_corr4 float, sci_moteopd_corr5 float,\
sci_moteopd_corr6 float, sci_moteopd_corr7 float, sci_moteopd_corr8 float,\
sci_moteopd_corr9 float, sci_moteopd_corr10 float, sci_moteopd_corr11 float, \
m_avg_climb_rate float, m_avg_dive_rate float, m_lat float, m_lon float, \
m_tot_horz_dist float, m_avg_speed float, sci_moteopd_volt float)')
        cur.close()

#We only want to operate on NEW files, not files that we have processed
#2011-06-03 rdc@mote.org
#split into two routines parseTbd and parseSbd
#tbd files have science data and sbd files have m_ data

def parseScience(gliderTbdDir, fileType):
    """
    Module:     parseScience()
    Date:       2012-11-27
    Author:     rdc@mote.org
    Modified:   2013-10-15
        By:     rdc@mote.org
    Inputs:     None
    Outputs:    Inserts into SQLite database file
    Purpose:    Grabs a db cursor and calls dbd2asc for all non-parsed tbd
                files. Looks for 'sci_' and splits sensor list. Finds pos
                of sensors of interest so list can change if tbdlist.dat
                changes. Uses pos of SOI to store values in correct field.
    """

    cur = gliderlog.cursor()
    curParsed = gliderlog.cursor()
    sbdParsedStructure = 0
    #
    #Regex for lines beginning with m_present_time
    #m_present_time structure: '1277271054.36734
    pattMPresentTime = re.compile(r'1[0-9]{9}\.[0-9]{5}')
    #
    select = "select %sfile from %sFiles where parsed = 0 \
ORDER BY %sfile ASC" % (fileType, fileType, fileType)
    cur.execute(select)
    for(tbdFile) in cur:
        if DEBUG > 0:
            print "Running dbd2asc on %s/%s" % (gliderTbdDir, tbdFile[0])
        command = "/opt/dinkum/dbd2asc -c /opt/glider_data_pipeline/cache \
%s/%s" % (gliderTbdDir, tbdFile[0])
        p = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE,
stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        (sIn, sOut) = (p.stdin, p.stdout)
        myBuffer = sOut.xreadlines()
        #
        curInsert = gliderlog.cursor()
        curInsert.execute('begin')
        for (line) in myBuffer:
            #2011-06-02 rdc changed to sci_ to reflect new tbd file format
            #Look to see if line starts with "sci_"  -- assume that a list
            #of sensors follows. Split into sensors, create tbdSensors
            #table and store values in tbdSensorReadings.
            if (line.startswith("sci_")):
                #we need to split and create here, not print
                sensorList = line.split()
                if (DEBUG > 1):
                    print "sensorList: %s" % (sensorList)
                if DEBUG > 1:
                    for (sensor) in sensorList:
                        print "Factored %s" % (sensor)
                #Get order of sensors
                m_present_timePos = sensorList.index('sci_m_present_time')
                sci_water_tempPos = sensorList.index('sci_water_temp')
                sci_water_condPos = sensorList.index('sci_water_cond')
                sci_water_pressurePos = sensorList.index('sci_water_pressure')
                sci_bbfl2s_chlor_scaledPos = sensorList.index('sci_bbfl2s_chlor_scaled')

            if pattMPresentTime.search(line):
                index = 0
                matchobj = pattMPresentTime.search(line)
                if matchobj:
                    if DEBUG > 1:
                        print "MATCH ON pattMPresentTime"
                        print "m_present_time: %s" % matchobj.group(0)
                    m_present_time = float(matchobj.group(0))
                    line  = line.replace('NaN','nan')
                    sensorReadings = line.split()
                    if DEBUG > 1:
                        print sensorReadings
                sci_water_cond = float(sensorReadings[sci_water_condPos])
                sci_water_temp = float(sensorReadings[sci_water_tempPos])
                sci_water_pressure = float(sensorReadings[sci_water_pressurePos])
                sci_water_condRatio = (sci_water_cond*0.23302418791070513)
                #Salinity and density calculations and db update
            #
                if (sci_water_condRatio > 0 and sci_water_temp > 0 and \
sci_water_pressure > 0):
                    calc_salinity = sw.salt(sci_water_condRatio, \
sci_water_temp, sci_water_pressure)

                    #get most recent position
                    #[TO DO] Fudge it for now -- add to plotValues
                    lon = -82
                    lat = 27
                    absSalinty = gsw.SA_Sstar_from_SP(calc_salinity, \
sci_water_pressure, lon, lat)[0]
                    calc_density = (gsw.rho(absSalinty, sci_water_temp, \
sci_water_pressure) - 1000)
                else:
                    sci_water_condRatio = 'nan'
                    calc_salinity = 'nan'
                    calc_density = 'nan'
                    absSalinty = 'nan'

                curInsert.execute("INSERT INTO plotValues(m_present_time,\
m_depth, m_water_depth, sci_water_temp, sci_water_cond, sci_water_pressure,\
calc_salinity, absSalinty, calc_density,sci_bbfl2s_chlor_scaled, sci_moteopd_corr1,\
sci_moteopd_corr2,sci_moteopd_corr3,sci_moteopd_corr4,sci_moteopd_corr5,\
sci_moteopd_corr6,sci_moteopd_corr7,sci_moteopd_corr8,sci_moteopd_corr9,\
sci_moteopd_corr10,sci_moteopd_corr11,m_avg_climb_rate,m_avg_dive_rate,m_lat, \
m_lon, m_tot_horz_dist, m_avg_speed,sci_moteopd_volt) \
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", \
(sensorReadings[m_present_timePos],'nan','nan',\
sensorReadings[sci_water_tempPos],sci_water_condRatio,\
sensorReadings[sci_water_pressurePos],calc_salinity, \
absSalinty, calc_density,sensorReadings[sci_bbfl2s_chlor_scaledPos],0,0,0,0,0,0,0,0,0,0,0 \
,'nan','nan','nan','nan','nan','nan',0))
        curInsert.execute('commit')
        curInsert.close()
        select = "UPDATE %sFiles set parsed = '1' where %sfile = '%s'" \
% (fileType, fileType, tbdFile[0])
        curParsed.execute(select)
        curParsed.close()
    cur.close()

#######
#We only want to operate on NEW files, not files that we have processed
def parseGlider(gliderSbdDir, fileType):
    """
    Module:     parseGlider()
    Date:       2012-11-27
    Author:     rdc@mote.org
    Modified:   2013-10-15
        By:     rdc@mote.org
    Inputs:     None
    Outputs:    Inserts into SQLite database file
    Purpose:    Grabs a db cursor and calls dbd2asc for all non-parsed sbd
                files. Looks for 'c_' and splits sensor list. Finds pos
                of sensors of interest so list can change if sbdlist.dat
                changes. Uses pos of SOI to store values in correct field.
    """
    cur = gliderlog.cursor()
    curParsed = gliderlog.cursor()
    sbdParsedStructure = 0
    #
    #Regex for lines beginning with m_present_time
    #m_present_time structure: '1277271054.36734
    pattMPresentTime = re.compile(r'1[0-9]{9}\.[0-9]{5}')
    #
    select = "select %sfile from %sFiles where parsed = 0 \
ORDER BY %sfile ASC" % (fileType, fileType, fileType)
    cur.execute(select)
    for(sbdFile) in cur:
        if DEBUG > 0:
            print "Running dbd2asc on %s/%s" % (gliderSbdDir, sbdFile[0])
        command = "/opt/dinkum/dbd2asc -c /opt/glider_data_pipeline/cache \
%s/%s" % (gliderSbdDir, sbdFile[0])

        p = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE,
stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        (sIn, sOut) = (p.stdin, p.stdout)
        myBuffer = sOut.xreadlines()
        curInsert = gliderlog.cursor()
        curInsert.execute('begin')
        for (line) in myBuffer:
            #2011-06-02 rdc changed to sci_ to reflect new tbd file format
            #Look to see if line starts with "sci_"  -- assume that a list
            #of sensors follows. Split into sensors, create tbdSensors
            #table and store values in tbdSensorReadings.
            if (line.startswith("c_")) or (line.startswith("cc_")):
                #we need to split and create here, not print
                sensorList = line.split()
                if (DEBUG > 1):
                    print "sensorList: %s" % (sensorList)
                if DEBUG > 1:
                    for (sensor) in sensorList:
                        print "Factored %s" % (sensor)
                #Get order of sensors
                m_present_timePos = sensorList.index('m_present_time')
                m_depthPos = sensorList.index('m_depth')
                m_water_depthPos = sensorList.index('m_water_depth')
                #m_avg_climb_ratePos = sensorList.index('m_avg_climb_rate')
                #m_avg_dive_ratePos = sensorList.index('m_avg_dive_rate')
                m_lat = sensorList.index('m_lat')
                m_lon = sensorList.index('m_lon')
                #m_tot_horz_dist = sensorList.index('m_tot_horz_dist')
                m_tot_horz_dist = 0
                #m_avg_speed = sensorList.index('m_avg_speed')
                #m_avg_speed = 0
            if pattMPresentTime.search(line):
                index = 0
                matchobj = pattMPresentTime.search(line)
                if matchobj:
                    if DEBUG > 1:
                        print "MATCH ON pattMPresentTime"
                        print "m_present_time: %s" % matchobj.group(0)
                    m_present_time = float(matchobj.group(0))
                    line  = line.replace('NaN','nan')
                    sensorReadings = line.split()
                    if DEBUG > 1:
                        print sensorReadings

                curInsert.execute("INSERT INTO plotValues(m_present_time,\
m_depth, m_water_depth, sci_water_temp, sci_water_cond, sci_water_pressure,\
calc_salinity, absSalinty, calc_density,sci_bbfl2s_chlor_scaled,sci_moteopd_corr1,\
sci_moteopd_corr2,sci_moteopd_corr3,sci_moteopd_corr4,sci_moteopd_corr5,\
sci_moteopd_corr6,sci_moteopd_corr7,sci_moteopd_corr8,sci_moteopd_corr9,\
sci_moteopd_corr10,sci_moteopd_corr11,m_avg_climb_rate,m_avg_dive_rate, \
m_lat, m_lon, m_tot_horz_dist, m_avg_speed,sci_moteopd_volt) \
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", \
(sensorReadings[m_present_timePos],sensorReadings[m_depthPos],\
sensorReadings[m_water_depthPos],'nan','nan','nan','nan',\
'nan','nan', 'nan', 'nan','nan', 'nan','nan', 'nan','nan',\
'nan', 'nan','nan', 'nan','nan',0,\
0, sensorReadings[m_lat],sensorReadings[m_lon],\
 0, 0,'nan'))
        curInsert.execute('commit')
        curInsert.close()

        select = "UPDATE %sFiles set parsed = '1' where %sfile = '%s'" \
% (fileType, fileType, sbdFile[0])
        curParsed.execute(select)
    curParsed.close()
    cur.close()

def listLogFiles(gliderLogDir, fileType):
    """
    Module:     listLogFiles()
    Date:       2012-11-27
    Author:     rdc@mote.org
    Modified:   2013-10-01
        By:     rdc@mote.org
    Inputs:     gliderLogDir, fileType
    Outputs:    List of files to be processed
    Purpose:    Looks in appropriate directory and scans for files that
                have not been parsed. Returns list of unparsed files..
    """

    print "listLogFiles(%s)" % (fileType)
    cur = gliderlog.cursor()
    fileCur = gliderlog.cursor()
    os.chdir(gliderLogDir)
    fileNames = glob.glob("*." + fileType)
    fileCount = len(fileNames)
    fileNumber = 0

    for file in fileNames:
        if(DEBUG > 0):
            print "File %d %s" % (fileNumber, file)
            fileNumber += 1
        haveFile = 0
        if fileType == "log":
            select = "select logfile from surfaceLogFiles where \
logfile = '%s'" % file
            fileCur.execute(select)
            for(logfile) in fileCur:
                if(logfile !=  ""):
                    if(DEBUG > 0):
                        print "%s already inserted..." % file
                    haveFile = 1
            if(haveFile == 0):
                if(DEBUG > 0):
                    print "Inserting " + file + " into surfaceLogFiles"
                cur.execute("INSERT into surfaceLogFiles(logfile, parsed) \
VALUES (?, ?)", (file, 0 ))

        if fileType == "tbd":
            select = "select tbdfile from tbdFiles where tbdfile = '%s'" % file
            fileCur.execute(select)
            for(tbdfile) in fileCur:
                if(tbdfile !=  ""):
                    if(DEBUG > 0):
                        print "%s already dinkumProcessed" % file
                    haveFile = 1

            if(haveFile == 0):
                if (fileType == "tbd"):
                    if(DEBUG > 0):
                        print "Inserting " + file + " into tbdFiles"
                    cur.execute("INSERT into tbdFiles(tbdfile, parsed) \
VALUES (?, ?)", (file, 0 ))


        if fileType == "sbd":
            select = "select sbdfile from sbdFiles where sbdfile = '%s'" % file
            fileCur.execute(select)
            for(sbdfile) in fileCur:
                if(sbdfile !=  ""):
                    if(DEBUG > 0):
                        print "%s already dinkumProcessed" % file
                    haveFile = 1

            if(haveFile == 0):
                if (fileType == "sbd"):
                    if(DEBUG > 0):
                        print "Inserting " + file + " into sbdFiles"
                    cur.execute("INSERT into sbdFiles(sbdfile, parsed) \
VALUES (?, ?)", (file, 0 ))

        if fileType == "dbd":
            select = "select dbdfile from dbdFiles where dbdfile = '%s'" % file
            fileCur.execute(select)
            for(dbdfile) in fileCur:
                if(dbdfile !=  ""):
                    if(DEBUG > 0):
                        print "%s already dinkumProcessed" % file
                    haveFile = 1

            if(haveFile == 0):
                if (fileType == "dbd"):
                    if(DEBUG > 0):
                        print "Inserting " + file + " into dbdFiles"
                    cur.execute("INSERT into dbdFiles(dbdfile, parsed) \
VALUES (?, ?)", (file, 0 ))


        if fileType == "ebd":
            select = "select ebdfile from ebdFiles where ebdfile = '%s'" % file
            fileCur.execute(select)
            for(ebdfile) in fileCur:
                if(ebdfile !=  ""):
                    if(DEBUG > 0):
                        print "%s already dinkumProcessed" % file
                    haveFile = 1

            if(haveFile == 0):
                if (fileType == "ebd"):
                    if(DEBUG > 0):
                        print "Inserting " + file + " into ebdFiles"
                    cur.execute("INSERT into ebdFiles(ebdfile, parsed) \
VALUES (?, ?)", (file, 0 ))
    cur.close()
    return (fileCount)

def genSensor(sensor, sensorType):
    """
    Name:       genSensor()
    Author:     rdc@mote.org
    Date:       04 Jume, 2013
    Updated:    04 June, 2013
    Purpose:    builds mySensorArray[]
    Inputs:     None
    Outputs:    returns mySensorArry
    """
    cur = gliderlog.cursor()
    mySensorArray = np.array([])

    print "genSensor(%s)" % (sensor)


    #Don't need starting element as we are all np.array now
    sensorQuery = "select %s from plotValues ORDER BY m_present_time ASC"\
 % sensor
    tempArray = []
    myData = cur.execute(sensorQuery)
    for (data) in myData:
        if data[0] == 'nan':
            tempArray.append(np.nan)
        else:
            tempArray.append(data[0])
    mySensorArray = np.array(tempArray)

    if DEBUG > 0:
        print "%s has %d elements" % (sensor, len(mySensorArray))
        print "%s Min: %0.4f" % (sensor, np.nanmin(mySensorArray))
        print "%s Max: %0.4f" % (sensor, np.nanmax(mySensorArray))
        print "---------------------------------------------------"
    return mySensorArray

def removeOutliers():
    """
    Module:     removeOutliers()
    Date:       2012-11-27
    Author:     rdc@mote.org
    Modified:   2013-10-01
        By:     rdc@mote.org
    Inputs:     None
    Outputs:    None
    Purpose:    Does rudimentary qa/qc on glider data.
    """

    cur = gliderlog.cursor()
    #clean shit up
    print "Removing outliers..."
    cur.execute("update plotValues set sci_water_temp = 'nan' \
where (sci_water_temp <= 0.0 and sci_water_temp != 'nan')")
    cur.execute("update plotValues set sci_water_temp = 'nan' \
where (sci_water_temp > 50.0 and sci_water_temp != 'nan')")
    cur.execute("update plotValues set m_water_depth = 'nan' \
where (m_water_depth <= 0.0 and m_water_depth != 'nan')")
    cur.execute("update plotValues set m_water_depth = 'nan' \
where (m_water_depth  > 100.0 and m_water_depth != 'nan')")
    cur.execute("update plotValues set m_depth = 'nan' \
where (m_depth <= 0.0 and m_depth != 'nan')")
    cur.execute("update plotValues set m_depth = 'nan' \
where (m_depth  > 40.0 and m_depth != 'nan')")
    cur.execute("update plotValues set sci_water_pressure = 'nan' \
where (sci_water_pressure > 10.0 and sci_water_pressure != 'nan')")
    cur.execute("update plotValues set sci_water_pressure = 'nan' \
where (sci_water_pressure < 0.0 and sci_water_pressure != 'nan')")
    cur.execute("update plotValues set sci_bbfl2s_chlor_scaled = 'nan' where sci_bbfl2s_chlor_scaled < 0.0 and sci_bbfl2s_chlor_scaled != 'nan'")
    cur.execute("update plotValues set sci_bbfl2s_chlor_scaled = 'nan' where sci_bbfl2s_chlor_scaled > 100.0 and sci_bbfl2s_chlor_scaled != 'nan'")

    #remove 1.0 and -1.0 SimIndexes
    for corr, species in speciesDict.iteritems():
        queryString = "update plotValues set %s = 'nan' where %s == 1.0" \
% (corr, corr)
        cur.execute(queryString)
        queryString = "update plotValues set %s = 'nan' where %s == -1.0" \
% (corr, corr)
        cur.execute(queryString)
    cur.close()


def plotSensor(sensor, dataArray, m_present_time, m_depth, m_water_depth, \
sensorString, glider):
    """
    Name:       plotSensor()
    Author:     rdc@mote.org
    Date:       29 May, 2013
    Updated:    03 June, 2013
    Purpose:    plots temp
    Inputs:     None
    Outputs:    sensor.png
    """
    print "plotSensor(%s)" % sensor
    #get sensor min/max/elements
    start_date, end_date = genStartEnd()

    sensorMin = np.nanmin(dataArray)
    sensorMax = np.nanmax(dataArray)
    m_water_depthMin = np.nanmin(m_water_depth)
    m_water_depthMax = np.nanmax(m_water_depth)
    m_depthMin = np.nanmin(m_depth)
    m_depthMax = np.nanmax(m_depth)
    if DEBUG > 0:
        print "plotSensor(%s) has %d elements" % (sensor, len(dataArray))
        print "plotSensor(m_depth) has %d elements" % (len(m_depth))
        print "plotSensor(m_water_depth) has %d elements" % (len(m_water_depth))
        print "plotSensor(%s) Min: %0.4f" % (sensor, np.nanmin(dataArray))
        print "plotSensor(%s) Max: %0.4f" % (sensor, np.nanmax(dataArray))
        print "plotSensor(m_depth) Max: %0.4f" % (m_depthMax)
        print "plotSensor(m_depth) Min: %0.4f" % (m_depthMin)
        print "plotSensor(m_water_depth) Min: %0.4f" % (m_water_depthMin)
        print "plotSensor(m_water_depth) Max: %0.4f" % (m_water_depthMax)

    #From plot.py of m_avg_climb_rate
    fig = plt.figure(figsize=(12, 6))
    daysFmt = mpd.DateFormatter('%m-%d')
    hoursFmt = mpd.DateFormatter('%H:%M')
    plt.rcParams['xtick.major.pad'] = '5'

    moteLogo = plt.imread(logoFile)
    #title and subtitle
    titleString = glider + " " + start_date + " GMT to " + end_date + " GMT\n"
    plt.title(titleString, fontsize=14, horizontalalignment = 'center')

    subTitleString = "%s" % (sensorString)
    plt.suptitle(subTitleString, fontsize=12, x=.46, y=.93, style='italic')
    plt.ylim(0, m_water_depthMax + 3)
    #labels
    plt.ylabel('Depth (m)')
    ax = plt.gca()


    #From plot.py of m_avg_climb_rate
    ax.xaxis.set_major_locator(mpd.DayLocator(interval=1))
    #ax.xaxis.set_minor_locator(mpd.HourLocator(interval=12))
    ax.xaxis.set_major_formatter(daysFmt)
    #ax.xaxis.set_minor_formatter(hoursFmt)
    ax.set_xlabel('Date/Time')
    #
    #Scatter Plot -- We need to build color map as dataArray can have nan values
    #which cause scatter to barf

    #We need to do this for m_depth and m_water_depth, not sensor
    #First we genSensor for m_depth, m_water_depth and Sensor
    #Then we interp m_depth and m_water_depth
    #
    #Then we blow array elements away where np.isnan(sensorReading)
    #What is left is same size m_depth, m_water_depth, dataArray with no NaNs
    #We interp so we have max data density for m_depth and m_water_depth

    #m_depth
    mask = np.isnan(m_depth)
    m_depth[mask] = np.interp(np.flatnonzero(mask), np.flatnonzero(~mask), \
m_depth[~mask])
    #m_water_depth
    mask = np.isnan(m_water_depth)
    m_water_depth[mask] = np.interp(np.flatnonzero(mask), \
np.flatnonzero(~mask), m_water_depth[~mask])

    nanElements = np.array([])
    if DEBUG > 0:
        print "Finding NaNs in dataArray..."
    nanElements = np.isnan(dataArray)

    #clean m_depth
    if DEBUG > 0:
        print "Cleaning out NaNs..."

    m_present_time = np.delete(m_present_time, nanElements, axis=0)
    m_depth = np.delete(m_depth, nanElements, axis=0)
    m_water_depth = np.delete(m_water_depth, nanElements, axis=0)
    dataArray = np.delete(dataArray, nanElements, axis=0)
    plt.scatter(mpd.epoch2num(m_present_time), m_depth, \
c=dataArray, s=20, lw=0)
    #From plot.py of m_avg_climb_rate
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=90)
    plt.setp(ax.xaxis.get_majorticklabels(), fontsize=12)
    plt.setp(ax.xaxis.get_minorticklabels(), rotation=90)
    plt.setp(ax.xaxis.get_minorticklabels(), fontsize=10)
    #
    #invert y axis and show colorbar
    plt.gca().invert_yaxis()
    #if OPD hardwire colorbar to min/max of SI
    if not 'sci_moteopd_corr' in sensor:
        plt.clim(sensorMin, sensorMax+.1)
        plt.colorbar()
    else:
        plt.clim(-1, 1)
        plt.colorbar()
    #We now plot bottom profile with OPD
    plt.scatter(mpd.epoch2num(m_present_time), m_water_depth, \
marker='o', c='k', s=5, lw=0)
    #add logo
    fig.figimage(moteLogo, 160, 60, zorder=10)
    #write file
    if not 'sci_moteopd_corr' in sensor:
        sensorFile = plots_directory + '/%s.png' % sensor
        print sensorFile
    else:
        sensorFile = plots_directory + '/species/%s.png'\
 % sensor
        print sensorFile
    if DEBUG > 0:
        print "plotSensor(): writing %s.png" % sensor
    plt.savefig(sensorFile, dpi=100)
    #clear figure for next plot
    plt.clf()
    plt.close()

def plotDiveClimbRates(sensor):
    """
    Name:       plotDiveClimb()
    Author:     rdc@mote.org
    Date:       11 Nov, 2013
    Updated:    11 Nov, 2013
    Purpose:    plots m_avg_dive and m_avg_climb as scatter
    Inputs:     None
    Outputs:    png file
    """
    cur = gliderlog.cursor()
    mySensorArray = []
    m_present_timeArray = []
    start_date, end_date = genStartEnd()

    #Plot entire mission without hour markers (too crowded)
    sensorQuery = "select m_present_time, %s from plotValues\
  ORDER BY m_present_time" % sensor
    myData = cur.execute(sensorQuery)
    for (m_present_time, reading ) in myData:
        if reading != 'nan':
            m_present_timeArray.append(m_present_time)
            mySensorArray.append(float(reading))

    m_present_timeArray = np.array(m_present_timeArray)
    mySensorArray = np.array(mySensorArray)

    fig = plt.figure(figsize=(24, 12))
    daysFmt = mpd.DateFormatter('%m-%d')
    hoursFmt = mpd.DateFormatter('%H:%M')
    ax = plt.gca()
    ax.xaxis.set_major_locator(mpd.DayLocator(interval=1))
    #ax.xaxis.set_minor_locator(mpd.HourLocator(interval=4))
    ax.xaxis.set_major_formatter(daysFmt)
    #ax.xaxis.set_minor_formatter(hoursFmt)
    ax.set_xlabel('Date/Time')
    #title and subtitle
    titleString = glider_name + " " + start_date + \
"  GMT to " + end_date + " GMT\n"
    plt.title(titleString, fontsize=14, horizontalalignment = 'center')
    plt.title(titleString, fontsize=18)
    subTitleString = "%s" % (sensor)
    plt.suptitle(subTitleString, fontsize=14, x=.44, \
y=.92, style='italic')
    plt.ylabel(sensor)
    plt.scatter(mpd.epoch2num(m_present_timeArray), mySensorArray, \
c=mySensorArray)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=90)
    plt.setp(ax.xaxis.get_majorticklabels(), fontsize=14)
    plt.setp(ax.xaxis.get_minorticklabels(), rotation=90)
    plt.setp(ax.xaxis.get_minorticklabels(), fontsize=10)
    print "plotDiveClimbRates(%s)" % sensor
    cax = cm.ScalarMappable(cmap='jet')
    cax.set_array(mySensorArray)
    fig.colorbar()
    plots_directory = dataDir + 'plots'
    sensorFile = plots_directory + '/%s.png' % sensor
    plt.savefig(sensorFile, dpi=100)
    #clear figure for next plot
    plt.clf()
    plt.close()

def genStartEnd():
    """
    Name:       genStartEnd()
    Author:     rdc@mote.org
    Date:       29 May, 2013
    Updated:    21 Nov, 2013
    Purpose:    gets start/end dates
    Inputs:     None
    Outputs:    start_date, end_date
    """
    moteCur = gliderlog.cursor()
    #get start/end dates

    myQuery = "select strftime(\"%Y-%m-%d %H:%M\",m_present_time,\
'unixepoch') from plotValues where m_present_time != 'nan' order by \
m_present_time ASC limit 1"
    moteCur.execute(myQuery)

    for (data) in moteCur:
        start_date = data[0]

    myQuery = "select strftime(\"%Y-%m-%d %H:%M\",m_present_time, \
'unixepoch') from plotValues where m_present_time !='nan' order by \
m_present_time DESC limit 1"
    moteCur.execute(myQuery)
    for (data) in moteCur:
        end_date = data[0]
    moteCur.close()
    return start_date, end_date

def genCSV():
    print "genCSV()"
    moteCur = gliderlog.cursor()

    myQuery = "select strftime(\"%Y_%m_%d\",m_present_time, 'unixepoch') \
from plotValues ORDER BY m_present_time ASC LIMIT 1"
    moteCur.execute(myQuery)
    for (data) in moteCur:
        start_date = data[0]

    csvFile = glider_name + "_" + start_date + ".csv"

    myQuery = "select * from plotValues order by m_present_time ASC"
    moteCur.execute(myQuery)
    #lose the brackets
    sensorNames = list(map(lambda x: x[0], moteCur.description))
    sensorNames = str(', '.join(sensorNames))

    csvFile = open(dataDir + '/processed_data/'+ csvFile,'w')
    csvFile.write(sensorNames)
    csvFile.write('\n')
    for (row) in moteCur:
        #lose the parens
        row = str(row)[1:-1]
        csvFile.write(str(row) +'\n')
    csvFile.close()


if __name__ == '__main__':

    if len(sys.argv) != 5:
        print "Usage: make_unit_555_plots vehicle dataDir inMission DEBUG_LEVEL"
        sys.exit(1)
    else:
        glider_name = sys.argv[1]
        dataDir = sys.argv[2]
        plots_directory = dataDir + 'plots'
        inMission = int(sys.argv[3])
        DEBUG = int(sys.argv[4])
        gliderSbdDir = dataDir + "binary_files/sbd"
        gliderTbdDir = dataDir + "binary_files/tbd"
        gliderDbdDir = dataDir + "binary_files/dbd"
        gliderEbdDir = dataDir + "binary_files/ebd"

    beginTimeStamp = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    print "Started run: %s" % beginTimeStamp
    createDB()

    fileCount = 0
    if inMission == 1:
        fileCount = 0
        fileCount = listLogFiles(gliderSbdDir,"sbd")
        if fileCount > 0:
            parseGlider(gliderSbdDir, 'sbd')

        fileCount = 0
        fileCount = listLogFiles(gliderTbdDir,"tbd")
        if fileCount > 0:
            parseScience(gliderTbdDir, 'tbd')

    if inMission == 0:
        fileCount = 0
        fileCount = listLogFiles(gliderDbdDir,"dbd")
        if fileCount > 0:
            parseGlider(gliderDbdDir, 'dbd')

        fileCount = 0
        fileCount = listLogFiles(gliderEbdDir,"ebd")
        if fileCount > 0:
            parseScience(gliderEbdDir, 'ebd')



    if fileCount != 0:
        removeOutliers()
        m_present_time = genSensor('m_present_time', 'sbd')
        m_depth = genSensor('m_depth', 'sbd')
        m_water_depth = genSensor('m_water_depth', 'sbd')
        sci_water_temp = genSensor('sci_water_temp', 'tbd')
        sci_water_cond = genSensor('sci_water_cond', 'tbd')
        calc_salinity = genSensor('calc_salinity', 'tbd')
        calc_density = genSensor('calc_density', 'tbd')
        sci_water_pressure = genSensor('sci_water_pressure', 'tbd')
        sci_bbfl2s_chlor_scaled = genSensor('sci_bbfl2s_chlor_scaled', 'tbd')
        unitString = r"Temperature ($^\circ$C)"
        plotSensor('sci_water_temp', sci_water_temp, m_present_time, m_depth, \
m_water_depth, unitString, glider_name)
        plotSensor('calc_salinity', calc_salinity, m_present_time, m_depth, \
m_water_depth, "Salinity (PSU)", glider_name)
        plotSensor('calc_density', calc_density, m_present_time, m_depth, \
m_water_depth, "Sigma-t (kg/m$^{3}$)", glider_name)

        plotSensor('sci_bbfl2s_chlor_scaled', sci_bbfl2s_chlor_scaled, m_present_time, \
m_depth, m_water_depth, r"Chlorophyll (ug/L)", glider_name)

        #dive/climb rates
        plotDiveClimbRates('m_avg_climb_rate')
        plotDiveClimbRates('m_avg_dive_rate')

        endTimeStamp = time.strftime("%Y-%m-%d %H:%M:%S %Z")
        print "Completed run: %s" % endTimeStamp
    else:
        print "Nothing to process..."
