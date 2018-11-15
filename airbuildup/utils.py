#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import codecs
import json
import os
import time

from datetime import datetime as dt
from datetime import timedelta

import log
import requests

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway



# Codec
UTF8 = 'utf-8'

# Set logger
logger = log.getLogger()

'''
Add timestamp to a dictionary
:input, some dictionary, e.g.
{
    'key1': 'value1'
    ...
}
:output, the dictionary with a timestamp when we start process it, i.e. dt.now()
e.g.
{
    'timestamp': ''
    'key1': 'value1'
    ...
}
'''


def addTimestamp(dict):
    try:
        dict.update({'timestamp': dt.now().strftime("%Y-%m-%d %H:%M:%S")})
        return dict
    except Exception:
        return None


def getHistoryByWindow(result, windowDuration, slideDuration):
    return result.map(lambda nameTuple: (nameTuple[0], [nameTuple[1]])) \
        .reduceByKeyAndWindow(lambda x, y: x + y,
                              None,
                              windowDuration,
                              slideDuration)


def getStatusByWindow(result, windowDuration, slideDuration):
    return result.reduceByWindow(lambda x, y: y,
                                 None,
                                 windowDuration,
                                 slideDuration)


'''
Read timestamp from input value
:Input: a list with single json, which include field of timestamp
:Output: timestamp envolved in input, otherwise return now() so that present recored will be valid one
'''


def getTimestampFromValue(value):
    try:
        return value['timestamp']
    except Exception:
        return dt.now()


'''
Check the last key of input dict and returns true if the key contains substring of table name
'''


def isLastKeyContainsString(dict_x, table_name):
    if len(dict_x.keys()) > 0:
        key = list(dict_x.keys())[-1]
        if table_name in key:
            return True
    return False


def isDevMode(conf):
    try:
        return int(conf.get("DEV_MODE", False))
    except Exception:
        logger.warn("Error in read environment DEV_MODE. Using default False.")
        return False


def isEmpty(x):
    return ((x is None) or (x is u''))


def isEmptyList(_list):
    return ((_list is None) or (0 == len(_list)))


def isNotEmpty(x):
    return ((x is not None) and (x is not u''))


def isNotEmptyList(_list):
    return ((_list is not None) and (0 != len(_list)))


'''
Check if input x is the data we need
'''


# def isSpecifiedData(x, dataSourceName):
#     try:
#         info = getDataSourceInfo()
#         table_name = info.get(dataSourceName).get('table')
#         return isLastKeyContainsString(x, table_name)
#     except Exception as err:
#         return False


def isTimeOnInterval(interval):
    if interval == None or \
            not isinstance(interval, int) or \
            interval <= 0:
        logger.warn("The interval \"%r\" is invaild." % interval)
        return False

    if int(time.time()) % interval == 0:
        return True
    else:
        return False


def loadValueFromKafka(x):
    '''
    Simple method of loading data and decoding, without using protobuf.
    Index 1 means value of k,v from kafka.
    '''
    try:
        return loadJson(x[1].decode(UTF8))
    except Exception as err:
        return "Cannot load value from kafka: %s" % err


def loadJson(x):
    try:
        return json.loads(x)
    except Exception as err:
        return "Cannot parse into JSON: %s" % err


def postRequest(url, data):
    try:
        sendStatus(200, '0')
        sendStatus(int(dt.now().timestamp()), '1')
        data = {"Parcels": data}

        r = requests.post(url, data=data)
        return r.content
    except Exception as err:
        # Cannot logging in streaming pprint
        # When testing, try send error to monotoring web server
        # r = requests.post(url, data={'error': err})
        return None


"""
Input is process result for each RDD in DStream
"""


def printResult(result, filePath):
    return result.filter(isNotEmpty) \
        .repartition(1) \
        .map(lambda x: ("Writing to %s with result: %s" % (filePath, x))) \
        .pprint()





'''
Input dict format:
    {
    'tt_waybill_info*signin_tm': '08:10.0', 
    'tt_waybill_info*modified_tm': '51:16.0',
    ...
    }
Output dict format:
    {
    'signin_tm': '08:10.0', 
    'modified_tm': '51:16.0',
    ...
    }
'''


def removePrefixOfKey(dict):
    return {k.split('*')[-1]: dict.get(k) for k in dict.keys()}


def saveAsText(result, prefix):
    result.filter(isNotEmpty) \
        .repartition(1) \
        .map(lambda x: "%s\t%s" % (x[0], x[1])) \
        .saveAsTextFiles(prefix, "csv")


def spotDecoder(s):
    return s


def stringToDatetime(dtString):
    if isNotEmpty(dtString) and str == type(dtString):
        dtString = dt.strptime(dtString, '%Y-%m-%d %H:%M:%S')
    return dtString


def stringToFloat(string):
    try:
        return float(string)
    except:
        return None


'''
:timestamp, string or other, e.g. '1.43653E+12'
:output, python datetime, e.g. 2015-07-10 20:06:40
'''


def timestampToDatetime(timestamp):
    try:
        return dt.fromtimestamp(int(stringToFloat(timestamp) / 1000.0))
    except:
        return dt.now() - timedelta(days=10 * 365)


def valueFromListToElement(kvs):
    try:
        return (kvs[0], kvs[1][0])
    except Exception:
        return kvs


def postStatus(status, url, jobName, tag):
    registry = CollectorRegistry()
    g = Gauge('airbuildup_status_0_' + tag, 'status', registry=registry)
    g.set(status)

    push_to_gateway(url, job=jobName, registry=registry)

def sendStatus(status, label):
    conf = readConfig().get("airbuildup")
    url = conf.get('MONITOR_URL', '10.116.115.69:9091')
    jobName = conf.get("APP_NAME", "airbuildup").lower()+"_"+label
    # tag = conf.get("DEV_MODE", "0") + "_" + label
    tag =label
    postStatus(status, url, jobName, tag)

def estimateCode(codeNum,dict):
    return (codeNum == dict.get('opCode'))

def isLoadGoodsOperation(dict):

    return estimateCode('30',dict)

def isUnlockVehicleOperation(dict):

    return estimateCode('37',dict)

def isAirRelatedOperation(dict):

    return estimateCode('67',dict)

def isAirFlyOperation(dict):

    return estimateCode('105',dict)

def isAirBoardOperation(dict):

    return estimateCode('75530', dict)










