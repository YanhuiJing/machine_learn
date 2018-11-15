#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from datetime import datetime as dt
from utils import isNotEmpty,addTimestamp, loadJson, readConfig,\
               isLoadGoodsOperation,isUnlockVehicleOperation,\
               isAirRelatedOperation,isAirFlyOperation
from google.protobuf.json_format import MessageToJson
import fact_route_v2_pb2 as factRoute

from config import conf

"""
    巴枪数据流转换:
        1,对把枪流进行反序列化操作,通过操作网点过滤深圳本地操作数据
        2,根据不同的过滤条件,分别获取装车数据、封车数据、解封车数据、打板数据、航空关联数据、航空起飞数据
        3,将数据转换为以运单为key,运单操作信息为value的Tuple返回数据
"""
def convertToBargunState(bargunDStream):

    bargunDStream = bargunDStream.map(loadBargunDataByDeserializer).\
                                    map(loadJson).\
                                    filter(isFromProvince)

    loadGoodsStream=bargunDStream.filter(isLoadGoodsOperation).\
                                  filter(isArrive755R).\
                                  map(selectLoadGoodsFields).\
                                  map(addTimestamp).\
                                  map(convertLoadGoodsKVpair)


    vehicleStream=bargunDStream.filter(isUnlockVehicleOperation()).\
                                    map(is755R).\
                                    map(selectVehicleFields).\
                                    map(addTimestamp).\
                                    map(convertWaybillKVpair)



    airStream755R = bargunDStream.filter(is755R)

    # airBoardStream=airStream755R.filter(isLoadGoodsOperation).\
    #                             map(selectAirBoardFields).\
    #                             map(addTimestamp).\
    #                             map(convertWaybillKVpair)


    airRelatedStream= airStream755R.filter(isAirRelatedOperation).\
                                map(selectAirRelatedFields). \
                                filter(getAirTransFull). \
                                map(addTimestamp).\
                                map(convertWaybillKVpair)


    airFlyStream=airStream755R.filter(isAirFlyOperation).\
                                map(selectAirFlyFields).\
                                map(addTimestamp).\
                                map(convertWaybillKVpair)


    return loadGoodsStream,vehicleStream,airRelatedStream,airFlyStream


def loadBargunDataByDeserializer(x):
    try:
        message = factRoute.FactRouteDto()
        message.ParseFromString(x[1])
        jsonMessage = MessageToJson(message)
        return jsonMessage
    except Exception as err:
        return "Cannot load value from kafka: %s" % err

def isFromProvince(dict):
    try:
        flag=False
        res=conf.get("PRESENT_PROVINCE_CITY_CODE","020,660,662,663,668,750,751,752,753,754,755,756,757,758,759,760,762,763,766,768,769")
        citys=res.split(",")
        for city in citys:
            if (city in dict.get('zoneCode')):
                return True
        return flag
    except Exception:
        return False


"""
    将线路编码包含755R的过滤出来
"""
def isArrive755R(dict):
    flag=False
    linecode=dict.get('opAttachInfo')
    if isNotEmpty(linecode):
        flag=("755R" in linecode[3:])
    return flag


def is755R(dict):
    flag=False
    zoneCode=dict.get('zoneCode')
    if "755R" == zoneCode:
        flag=True
    return flag


def selectLoadGoodsFields(dict):
    try:
        return {
                    'opCode':dict.get('opCode'),
                    'opName':dict.get('opName'),
                    'waybillNo':dict.get('mainWaybillNo'),
                    'lineCode':dict.get('opAttachInfo'),
                    'zoneCode':dict.get('zoneCode'),
                    'barScanTm':dict.get('barScanTm')
                    }
    except Exception:
        return None

"""
    车标流包含封车操作和解封车操作
"""
def selectVehicleFields(dict):
    try:
        return {
                    'opCode':dict.get('opCode'),
                    'opName':dict.get('opName'),
                    'waybillNo': dict.get('mainWaybillNo'),
                    'zoneCode':dict.get('zoneCode'),
                    'barScanTm':dict.get('barScanTm')
                    }
    except Exception:
        return None


def selectAirBoardFields(dict):
    try:
        return {
                    'opCode':'75530',
                    'waybillNo':dict.get('mainWaybillNo'),
                    'airlineCode':dict.get('opAttachInfo'),
                    'zoneCode':dict.get('zoneCode'),
                    'barScanTm':dict.get('barScanTm')
                    }
    except Exception:
        return None


def selectAirRelatedFields(dict):
    try:
        return {
            'opCode': dict.get('opCode'),
            'waybillNo': dict.get('mainWaybillNo'),
            'airBoardNum': dict.get('opAttachInfo'),
            'zoneCode': dict.get('zoneCode'),
            'barScanTm': dict.get('barScanTm'),
            'airTransType':dict.get('otherInfo'),
            'airPlanNum':dict.get('extendAttach1'),
            'airTaskId':dict.get('extendAttach2')
        }
    except Exception:
        return None


def selectAirFlyFields(dict):
    try:
        return {
            'opCode': dict.get('opCode'),
            'waybillNo': dict.get('mainWaybillNo'),
            'barScanTm': dict.get('barScanTm'),
        }
    except Exception:
        return None


def addTimestamp(dict):
    try:
        dict.update({'timestamp': dt.now()})
        return dict
    except Exception:
        return None

def getAirTransFull(line):
    try:
        if line.get("airTransType") == "1":
            return True
        else:
            return False
    except Exception:
        return False

"""
    装车流需要转换成以线路编码为key,运单信息为value的元祖格式,
    通过线路编码与运力数据进行关联
"""
def convertLoadGoodsKVpair(dict):
    try:
        return (dict.get('lineCode', None), dict)
    except Exception:
        return None


def convertWaybillKVpair(dict):
    try:
        return (dict.get('waybillNo',None),dict)
    except Exception:
        return None







