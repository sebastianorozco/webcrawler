"""
crawler.py
"""

from urlparse import urljoin,urlparse
from collections import deque
import re
import traceback
from locale import getdefaultlocale
import logging
import time

from bs4 import BeautifulSoup 

from database import Database
from webPage import WebPage
from threadPool import ThreadPool

log = logging.getLogger('Main.crawler')


class Crawler(object):

    def __init__(self, args):
        self.depth = args.depth  
        self.currentDepth = 1  
        self.keyword = args.keyword.decode(getdefaultlocale()[1]) 
        self.database =  Database(args.dbFile)
        self.threadPool = ThreadPool(args.threadNum)  
        self.visitedHrefs = set()   
        self.unvisitedHrefs = deque()    
        self.unvisitedHrefs.append(args.url) 
        self.isCrawling = False

    def start(self):
        print '\nStart Crawling\n'
        if not self._isDatabaseAvaliable():
            print 'Error: Unable to open database file.\n'
        else:
            self.isCrawling = True
            self.threadPool.startThreads() 
            while self.currentDepth < self.depth+1:
                self._assignCurrentDepthTasks ()
                #self.threadPool.taskJoin()Ctrl-C Interupt
                while self.threadPool.getTaskLeft():
                    time.sleep(8)
                print 'Depth %d Finish. Totally visited %d links. \n' % (
                    self.currentDepth, len(self.visitedHrefs))
                log.info('Depth %d Finish. Total visited Links: %d\n' % (
                    self.currentDepth, len(self.visitedHrefs)))
                self.currentDepth += 1
            self.stop()

    def stop(self):
        self.isCrawling = False
        self.threadPool.stopThreads()
        self.database.close()

    def getAlreadyVisitedNum(self):
        #visitedHrefstaskQueue
        visitedHrefs
        return len(self.visitedHrefs) - self.threadPool.getTaskLeft()

    def _assignCurrentDepthTasks(self):
        while self.unvisitedHrefs:
            url = self.unvisitedHrefs.popleft()
            self.threadPool.putTask(self._taskHandler, url)   
            self.visitedHrefs.add(url)  
 
    def _taskHandler(self, url):
        webPage = WebPage(url)
        if webPage.fetch():
            self._saveTaskResults(webPage)
            self._addUnvisitedHrefs(webPage)

    def _saveTaskResults(self, webPage):
        url, pageSource = webPage.getDatas()
        try:
            if self.keyword:
                if re.search(self.keyword, pageSource, re.I):
                    self.database.saveData(url, pageSource, self.keyword) 
            else:
                self.database.saveData(url, pageSource)
        except Exception, e:
            log.error(' URL: %s ' % url + traceback.format_exc())

    def _addUnvisitedHrefs(self, webPage):
        url, pageSource = webPage.getDatas()
        hrefs = self._getAllHrefsFromPage(url, pageSource)
        for href in hrefs:
            if self._isHttpOrHttpsProtocol(href):
                if not self._isHrefRepeated(href):
                    self.unvisitedHrefs.append(href)

    def _getAllHrefsFromPage(self, url, pageSource):
        hrefs = []
        soup = BeautifulSoup(pageSource)
        results = soup.find_all('a',href=True)
        for a in results:
            href = a.get('href').encode('utf8')
            if not href.startswith('http'):
                href = urljoin(url, href)
            hrefs.append(href)
        return hrefs

    def _isHttpOrHttpsProtocol(self, href):
        protocal = urlparse(href).scheme
        if protocal == 'http' or protocal == 'https':
            return True
        return False

    def _isHrefRepeated(self, href):
        if href in self.visitedHrefs or href in self.unvisitedHrefs:
            return True
        return False

    def _isDatabaseAvaliable(self):
        if self.database.isConn():
            return True
        return False

    def selfTesting(self, args):
        url = 'http://www.baidu.com/'
        print '\nVisiting www.baidu.com'
        pageSource = WebPage(url).fetch()
        if pageSource == None:
            print 'Please check your network and make sure it\'s connected.\n'
        elif not self._isDatabaseAvaliable():
            print 'Please make sure you have the permission to save data: %s\n' % args.dbFile
        else:
            self._saveTaskResults(url, pageSource)
            print 'Create logfile and database Successfully.'
            print 'Already save Baidu.com, Please check the database record.'
            print 'Seems No Problem!\n'
