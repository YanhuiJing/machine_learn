#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from utils import isEmpty, isNotEmpty, isNotEmptyList, loadValueFromKafka, valueFromListToElement

from datetime import datetime as dt

from config import conf

"""
1,通过过滤运单时效,获取t4-t8类快件,过滤空运快件
2,通过目的地过滤省外快件,获取空运件
"""


def parseWaybill(waybillDStream, zoneHubDStream):
    zoneToHubStateDStream = zoneHubDStream.map(constructZoneToHub). \
        filter(isNotEmpty). \
        updateStateByKey(accumulatorForZoneHub). \
        map(valueFromListToElement)
    zoneToHubStateDStream.checkpoint(conf.get("CHECKPOINT_INTERVAL", 100))
    zoneToHubStateDStream.count().map(lambda x: "counts of zoneToHubStateDStream: %s" % x).pprint()

    waybillDStream = waybillDStream.map(loadValueFromKafka). \
        filter(isFromProvince). \
        filter(isOutProvince). \
        filter(isAirTransformType). \
        map(selectFields). \
        map(convertWeight). \
        map(changeKeyToDestZoneCode). \
        filter(isNotEmpty).\
        leftOuterJoin(zoneToHubStateDStream). \
        map(combineWaybillAndHub). \
        map(constructKVpair)

    return waybillDStream


def constructZoneToHub(kvs):
    try:
        pair = kvs.split(',')
        return (pair[0], pair[1])
    except Exception:
        return None


def accumulatorForZoneHub(newValue, lastValue):
    if isNotEmptyList(newValue):
        return newValue
    else:
        return lastValue


'''
Used for a filter that fetch only packages pickup at Shenzhen
:Input: all waybill stream data
:Output: true when packages is picked up at Shenzhen

020,660,750,752,754,755,756,757,758,760,762,769

'''


def isFromProvince(dict):
    try:
        flag = False
        res = conf.get("PRESENT_PROVINCE_CITY_CODE",
                       "020,660,662,663,668,750,751,752,753,754,755,756,757,758,759,760,762,763,766,768,769")
        citys = res.split(",")
        for city in citys:
            if (city in dict.get('destZoneCode')):
                return True
        return flag
    except Exception:
        return False


def isOutProvince(dict):
    try:
        res = conf.get("PRESENT_PROVINCE_CITY_CODE",
                       "020,660,662,663,668,750,751,752,753,754,755,756,757,758,759,760,762,763,766,768,769")
        citys = res.split(",")
        for city in citys:
            if (city in dict.get('destZoneCode')):
                return False
        return True
    except Exception:
        return False


def isAirTransformType(dict):
    try:
        res = conf.get("AIR_TRANSFORM_TYPE", "T4,T801")
        isAirType = (dict.get("limitTypeCode") in res)
        return isAirType
    except Exception:
        return False


"""
    meterageWeightQty:计费总重量
    realWeightQty:实际重量
    quantity:包裹件数
    consignedTm:寄件时间
    limitTypeCode:时效类型
    distanceTypeCode:区域类型
    transportTypeCode:运输类型
"""


def selectFields(dict):
    try:

        consignedTm = dict.get('consignedTm')

        time = dt.strptime(consignedTm, "%Y-%m-%d %H:%M:%S")
        return {
            'waybillId': dict.get('waybillId'),
            'waybillNo': dict.get('waybillNo'),
            'destZoneCode': dict.get('destZoneCode'),
            'sourceZoneCode': dict.get('sourceZoneCode'),
            'meterageWeightQty': dict.get('meterageWeightQty'),
            'realWeightQty': dict.get('realWeightQty'),
            'consignedTm': dict.get('consignedTm'),
            'timestamp': time,
            'limitTypeCode': dict.get('limitTypeCode')
        }
    except Exception:
        return None


def convertWeight(dict):
    try:
        realWeightQty = dict.get('realWeightQty')

        if realWeightQty < 1:
            realWeightQty = realWeightQty
        elif realWeightQty == 1:
            realWeightQty = realWeightQty * 0.4
        else:
            realWeightQty = realWeightQty - 0.25

        dict['realWeightQty'] = realWeightQty

        return dict
    except Exception:
        return None


def constructKVpair(dict):
    try:
        return (dict.get('waybillNo'), dict)
    except Exception:
        return None


def changeKeyToDestZoneCode(dict):
    try:
        return (dict.get('destZoneCode', None), dict)
    except Exception:
        return None


def combineWaybillAndHub(kvs):
    try:
        valuePair = kvs[1]
        waybillValue = valuePair[0]
        hubValue = valuePair[1]
        if isNotEmpty(hubValue):
            waybillValue.update({'destGatewayHub': hubValue})
       
        return waybillValue
    except Exception:
        return None
