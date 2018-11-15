#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import datetime as dt
import inspect


class Logger():

    def __init__(self):
        pass

    def info(self, message):
        print(self.formatter('INFO', message))

    def error(self, message):
        print(self.formatter('ERROR', message))

    def debug(self, message):
        print(self.formatter('DEBUG', message))

    def warn(self, message):
        print(self.formatter('WARN', message))

    def formatter(self, level, message):
        asctime = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            threadName = inspect.getouterframes(inspect.currentframe(), 2)[2][3]
        except:
            threadName = 'UNKNOWN'
        return ("%s [%s] [%s]  %s" % (asctime, level, threadName, message))


def getLogger():
    return Logger()
