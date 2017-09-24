import logging
import time
from datetime import datetime
from threading import Thread

from crawler import Crawler
from options import parser


def congifLogger(logFile, logLevel):
    logger = logging.getLogger('Main')
    LEVELS={
        1:logging.CRITICAL, 
        2:logging.ERROR,
        3:logging.WARNING,
        4:logging.INFO,
        5:logging.DEBUG,
        }
    formatter = logging.Formatter(
        '%(asctime)s %(threadName)s %(levelname)s %(message)s')
    try:
        fileHandler = logging.FileHandler(logFile)
    except IOError, e:
        return False
    else:
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)
        logger.setLevel(LEVELS.get(logLevel))
        return True


class PrintProgress(Thread):

    def __init__(self, crawler):
        Thread.__init__(self)
        self.name = 'PrintProgress'
        self.beginTime = datetime.now()
        self.crawler = crawler
        self.daemon = True

    def run(self):
        while 1:
            if self.crawler.isCrawling:
                print '-------------------------------------------'
                print 'Crawling in depth %d' % self.crawler.currentDepth
                print 'Already visited %d Links' % self.crawler.getAlreadyVisitedNum()
                print '%d tasks remaining in thread pool.' % self.crawler.threadPool.getTaskLeft()
                print '-------------------------------------------\n'   
                time.sleep(10)

    def printSpendingTime(self):
        self.endTime = datetime.now()
        print 'Begins at :%s' % self.beginTime
        print 'Ends at   :%s' % self.endTime
        print 'Spend time: %s \n'%(self.endTime - self.beginTime)
        print 'Finish!'


def main():
    args = parser.parse_args()
    if not congifLogger(args.logFile, args.logLevel):
        print '\nPermission denied: %s' % args.logFile
        print 'Please make sure you have the permission to save the log file!\n'
    elif args.testSelf:
        Crawler(args).selfTesting(args)
    else:
        crawler = Crawler(args)
        printProgress = PrintProgress(crawler)
        printProgress.start()
        crawler.start()
        printProgress.printSpendingTime()


if __name__ == '__main__':
    main()
