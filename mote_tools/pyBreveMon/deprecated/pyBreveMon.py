#!/opt/ActivePython-2.5/bin/python
#
#Name:      pyBreveMon
#Author:    rdc@mote.org
#Version:   1.01a
#Date:      17 October 2007
#
#Note:
# Need to tweak date and time. Use localtime not
# recvd bb time as time on bb not always set properly.
# We don't really care what time the bb thinks it is, we
# care what time we got the data
#
# Need to do the query to get site value. Right now I'm
# Stuffing by hand. 
#
#25 October 2007 rdc@mote.org
#   Got site query working
#
#
#28 April 2008 rdc@mote.org
#added dataElements2 and < > checking  since we need to
#handle more than one model now
#
from twisted.internet import reactor
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
import re
import time 
import sys
import MySQLdb
import logging

class BreveLogger(LineReceiver):
    listenPort = 4001
    #listenPort = 4005
    logFile = "/var/log/pyBreveMon.log"
    dbHost = "coolcomms.mote.org"
    dbDB = "brevebuster"
    dbTable = "bbReports"
    dbUser = "breve"
    dbPass = "buster"

    logging.basicConfig(level=logging.INFO, filename='/var/log/pyBreveMon.log',format="%(asctime)s %(levelname)s %(message)s")
    pattBusterStatus = re.compile('^\*\*\*[0-9]+')
    dataElements = 14
    dataElements2 = 16
    logging.info('pyBreveMon ready for incoming connections on port %d',listenPort)
 
    def connectionMade(self):
        logging.warn('Accepted incoming connection from %s', self.transport.client)

    def connectionLost(self, reason):
        logging.warn('Closed connection from %s',self.transport.client)


    def lineReceived(self, line):
        #check for brevebuster status line formatting
        if self.pattBusterStatus.match(line):
            myStatus = line.split(',')
            if (len(myStatus) < self.dataElements):
                logging.error('Invalid <STATUS> line! Received %d fields')
                logging.error('<BAD STATUS>: %s', line)
            elif (len(myStatus) > self.dataElements2):
                logging.error('Invalid <STATUS> line! Received %d fields')
                logging.error('<BAD STATUS>: %s', line);
            else:
                logging.info('%s', line)
                #do the DB connect here
                db = MySQLdb.connect(host=self.dbHost, user=self.dbUser, passwd=self.dbPass,db=self.dbDB)
                cursor = db.cursor()
                #remove the leading ***
                line = line.replace('***','')
                line = line.split(',')
                #need to make the date format mySql style and also set
                #to localtime. BB clocks are sometimes off and we don't
                #care so much what time the BB thinks it is as much as 
                #when the data arrives
                myDate = time.strftime('%Y-%m-%d',time.localtime())
                myTime = time.strftime('%H:%M:%S',time.localtime())
                mySerial = int(line[0].strip())
                line[6] = myDate
                line[7] = myTime
                insertString = ""
                count = 1 
                for item in line:
                    if (count < len(line)):
                        insertString = insertString + "'"  + item.strip() + "',"  
                    else:
                        insertString = insertString + "'"  + item.strip() + "'"  
                    count += 1
               
                #get the site
                siteQuery = 'SELECT site FROM site where serial = %d' % (mySerial)
                result = cursor.execute(siteQuery)
                if (result == 1):
                    for(site) in cursor:
                        site = "%s" % site 
                        logging.info('Query for serial %d returned %s', mySerial,site)
                else:
                    pass
                    logging.warn('Query for serial %d failed to return a site',mySerial)
                    site = "NULL"

                #insertQuery = 'INSERT INTO brevebuster VALUES (%s,\'%s\')' % (insertString,site) 
                insertQuery = 'INSERT INTO bbReports VALUES (%s,\'%s\')' % (insertString,site) 
                result = cursor.execute(insertQuery)
                if (result == 1):
                    logging.info('Insert to table %s on server %s succeeded',self.dbTable,self.dbHost)
                else:
                    logging.error('Insert failed to table %s on server %s!',self.dbTable,self.dbHost)
        else:
            #just print it
            logging.info(line)

def main():
    factory = Factory()
    factory.protocol = BreveLogger
    reactor.listenTCP(BreveLogger.listenPort, factory)
    reactor.run()

main()


