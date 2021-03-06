import sqlite3

class Database(object):
    def __init__(self, dbFile):
        try:
            self.conn = sqlite3.connect(dbFile, isolation_level=None, check_same_thread = False) commit,
            self.conn.execute('''CREATE TABLE IF NOT EXISTS
                            Webpage (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                            url TEXT, 
                            pageSource TEXT,
                            keyword TEXT)''')
        except Exception, e:
            self.conn = None

    def isConn(self):
        if self.conn:
            return True
        else:
            return False

    def saveData(self, url, pageSource, keyword=''):
        if self.conn:
            sql='''INSERT INTO Webpage (url, pageSource, keyword) VALUES (?, ?, ?);'''
            self.conn.execute(sql, (url, pageSource, keyword) )
        else :
            raise sqlite3.OperationalError,'Database is not connected. Can not save Data!'

    def close(self):
        if self.conn:
            self.conn.close()
        else :
            raise sqlite3.OperationalError, 'Database is not connected.'
