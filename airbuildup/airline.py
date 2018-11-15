#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from datetime import datetime as dt
from datetime import timedelta

from utils import addTimestamp, getTimestampFromValue, \
                                isDevMode, isLastKeyContainsString, \
                                isEmpty, isEmptyList, \
                                isNotEmpty, isNotEmptyList, \
                                loadValueFromKafka, loadJson, \
                                readConfig, \
                                valueFromListToElement

conf = readConfig().get("airbuildup")

"""
{'cargo_city': '', 'lastest_arrive_tm': '2018-06-07 13:40:00', 'send_batch_tm': '2018-06-07 00:00:00', 'status': 0, 'line_scheme_plan': '1', 
'sub_line_id': 278024496, 'cvy_name': 'æ\x9d¨å±\x8bä¸\x8aå\x9cº1355', 'transfer_port': '', 'plan_arrive_tm': '2018-06-07 14:15:00', 
'cross_days': 0, 'type': 1, 'arrive_zone_code': '769TB', 'is_drop': 0, 'is_stop_over': 0, 'cargo_code': '769TB', 'full_load_weight': 2000.0, 
'trans_capacity_type': '4', 'plan_send_tm': '2018-06-07 13:55:00', 'src_batch_workday': '4', 'vehicle_num': 1, 'dest_batch_workday': '4', 
'versiondt': '2018-06-02 00:00:00', 'update_tm': '2018-05-31 22:03:22', 'pass_zone': None, 'src_zone_code': '769TA', 'arrive_batch_tm': '2018-06-07 00:00:00', 
'sub_sort': None, 'work_days': '4', 'cvy_capacity': 3000.0, 'line_require_date': '2018-06-07 00:00:00', 'line_key': '769TA769TB1355_769TA03D_769TB03D_0', 
'route_code': 'T1', 'main_line_id': None, 'send_zone_code': '769TA', 'is_sendomp': 0, 'line_code': '769TA769TB1355', 'job_type': 1, 'customs_decl': '',
'send_batch': '769TA03D', 'transport_level': 3, 'cargo_area': '', 'is_have_cargo': '0', 'arrive_batch': '769TB03D', 'is_subline': 0}
"""
def convertToLineState(lineDStream):
    lineDStream = lineDStream.map(loadValueFromKafka).\
                            filter(isFromLocal). \
                            map(selectFields). \
                            flatMap(splitDestCityList). \
                            filter(isNotEmpty). \
                            map(addTimestamp)



    lineStateDStream = lineDStream.map(constructKVpair). \
                                                        filter(isNotEmpty). \
                                                        updateStateByKey(accumulator). \
                                                        map(valueFromListToElement)

    lineStateDStream.checkpoint(conf.get("CHECKPOINT_INTERVAL", 100))


    return lineStateDStream

def isFromLocal(dict):
    try:
        isFromLocal = ("755R" == dict.get('send_zone_code'))
        return isFromLocal
    except Exception:
        return False

def selectFields(dict):
    try:
        return {
                    'line_code':dict.get('line_code'),
                    'depart_zone_code':dict.get('send_zone_code'),
                    'arrive_zone_code':dict.get('arrive_zone_code'),
                    'plan_depart_tm':dict.get('plan_send_tm'),
                    'lastest_arrive_tm':dict.get('lastest_arrive_tm'),
                    'route_code':dict.get('route_code'),
                    'dest_city_code':dict.get('cargo_city'),
                    }
    except Exception:
        return None

def splitDestCityList(dict):
    try:
        destCityList  = dict.get('dest_city_code').split(',')
        if len(destCityList) > 1:
            result = [generateNewDict(dict, _destCity) for _destCity in destCityList]
        else:
            result = [dict]
        return result
    except Exception:
        return [dict]

def generateNewDict(dict, destCity):
    if isNotEmpty(destCity):
        newDict = dict.copy()
        newDict['dest_city_code'] = destCity
    else:
        newDict = None
    return newDict

def constructKVpair(dict):
    try:
        return (dict.get('line_code') + '-' + dict.get('dest_city_code'), dict)
    except Exception:
        return None

def accumulator(newValue, lastValue):
    if isNotEmptyList(newValue):
        return newValue
    elif isNotEmptyList(lastValue) and isRecordsExpired(lastValue):
        return None
    else:
        return lastValue

def isRecordsExpired(lastValue):
    if isinstance(conf,dict):
        expireDays = conf.get("CONVEYANCE_EXPIRE_DAYS", 3)
    else:
        expireDays = 3

    return getTimestampFromValue(lastValue) < dt.now() - timedelta(days=expireDays)