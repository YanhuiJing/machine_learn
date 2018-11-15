#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from datetime import datetime as dt
from datetime import timedelta
from utils import isNotEmpty,isNotEmptyList,getTimestampFromValue, timestampToDatetime, \
    isEmpty,isAirBoardOperation,isAirRelatedOperation,isAirFlyOperation

from config import conf
"""
    聚合运单流,航空打板流,航空关联流,航空起飞流
    1,数据添加:

"""

def updateStateAirBoardingStream(waybillStream,airRelatedStream, airFlyStream):
    unionStream =waybillStream.union(airRelatedStream).union(airFlyStream)
    airRelatedStream.pprint()
    airRelatedStream.count().map(lambda line:"The count of airRelate is %s" % line).pprint()
    airBoardingState = unionStream.filter(isNotEmpty). \
                                updateStateByKey(accumulator). \
                                checkpoint(conf.get("CHECKPOINT_INTERVAL", 1000))

    packageState=airBoardingState.map(lambda kvs: kvs[1]).\
                                filter(lambda line:line.get('isAirBoard')==1)

    packageState.count().map(lambda line:"The count of boardingState is %s" % line).pprint()



    airBoardRes=packageState.map(convertAirboardingKVPairs). \
                reduceByKey(combineCountWeight).\
                map(convertLineToJson)

    return airBoardRes


"""
    把枪,运单流聚合更新
    1:首次更新,直接更新新值
    2:二次更新,根据不同的操作类型,更新数据
    3:过时数据清除,根据数据添加的时间戳,删除过期数据
    4:直接返回原有数据
"""
def accumulator(newValue, oldValue):
    if isNotEmptyList(newValue) and isEmpty(oldValue):
        return addNewPackage(newValue)
    elif isNotEmptyList(newValue) and isNotEmpty(oldValue):
        return updateOldPackage(newValue, oldValue)
    elif isNotEmpty(oldValue) and isRecordsExpired(oldValue):
       return None
    else:
        return oldValue

def isRecordsExpired(oldValue):

    expireDays = conf.get("PACKAGE_EXPIRE_DAYS", 1)

    return getTimestampFromValue(oldValue) < (dt.now() - timedelta(days=expireDays))


"""
    根据不同的操作类型添加新数据
"""
def addNewPackage(newValue):
    # 数据更新,同一时间周期相同key值的数据聚合在同一个集合内,选取
    lastData = {}
    for value in newValue:
        inputData = value

        if isWaybillOperation(inputData):
            lastData = updateOldPackageByWaybill(inputData, lastData)
        elif isAirRelatedOperation(inputData):
            lastData = addNewPackageByAirRelated(inputData, lastData)
        else:
            return None
    return lastData


def updateOldPackage(newValue, oldValue):
    lastData = oldValue
    for value in newValue:
        inputData = value

        if isAirRelatedOperation(inputData):
            lastData = updateOldPackageByAirRealted(inputData, lastData)
        elif isAirFlyOperation(inputData):
            return None
        elif isWaybillOperation(inputData):
            lastData = updateOldPackageByWaybill(inputData, lastData)
    return lastData



def updateOldPackageByWaybill(newValue, oldValue):
    try:
        oldValue.update({'waybillNo': newValue.get('waybillNo', None)})
        oldValue.update({'timestamp': newValue.get('timestamp', None)})
        oldValue.update({'realWeightQty': newValue.get('realWeightQty', None)})
        oldValue.update({'destZoneCode': newValue.get('destZoneCode', None)})
        oldValue.update({'limitTypeCode': newValue.get('limitTypeCode', None)})
        oldValue.update({'destGatewayHub':newValue.get('destGatewayHub', None)})
        return oldValue
    except Exception:
        return oldValue

def addNewPackageByAirRelated(newValue,oldValue):
    try:
        barScanTm = newValue.get('barScanTm', None)
        boardTime = timestampToDatetime(barScanTm)
        oldValue.update({'isAirBoard': 1})
        oldValue.update({'timestamp': newValue.get('timestamp', None)})
        oldValue.update({'airBoardNum': newValue.get('airBoardNum', None)})
        oldValue.update({'airTransType': newValue.get('airTransType', None)})
        oldValue.update({'airPlanNum': newValue.get('airPlanNum', None)})
        oldValue.update({'airTaskId': newValue.get('airTaskId', None)})
        oldValue.update({'boardTime': boardTime})
        return oldValue
    except Exception:
        print("****")
        return oldValue

def updateOldPackageByAirRealted(newValue, oldValue):
    try:
        if isNotEmpty(oldValue.get("boardTime")):
            barScanTm = newValue.get('barScanTm', None)
            boardTime = timestampToDatetime(barScanTm)
            oldTime=oldValue.get("boardTime")
            if oldTime<boardTime:
                return addNewPackageByAirRelated(newValue,oldValue)
            else:
                return oldValue
        else:
            return addNewPackageByAirRelated(newValue,oldValue)
    except Exception:
        print("""""")
        return oldValue

def isWaybillOperation(dict):
    flag=False

    if isNotEmpty(dict.get('waybillId', None)):
        return True

    return flag


def convertAirboardingKVPairs(dict):
    airBoardNum=dict.get("airBoardNum")
    airPlanNum=dict.get("airPlanNum")
    destGatewayHub=dict.get("destGatewayHub")
    boardTime=dict.get("boardTime")
    boardDate,boardShift=splitDateAndTime(boardTime)
    if isNotEmpty(dict.get("realWeightQty")):
        waybillWeight=float(dict.get("realWeightQty"))
    else:
        waybillWeight=0

    return ((airBoardNum,airPlanNum,destGatewayHub,boardDate,boardShift),(1,waybillWeight))

def splitDateAndTime(departTime):
    try:
        departDate = departTime.strftime("%Y-%m-%d")
        departHour = int(departTime.strftime("%H"))
        departMinute = int(departTime.strftime("%M"))
        shift_time = 5
        shift_num = 60 / shift_time
        shift_minute=int(departMinute / shift_time)
        if departHour < 12:
            date_temp=dt.strptime(departDate,"%Y-%m-%d")-timedelta(days=1)
            departDate=dt.strftime(date_temp,"%Y-%m-%d")
            departShift = (departHour + 12) * shift_num + shift_minute
        else:
            departShift = (departHour - 12) * shift_num + shift_minute
        return departDate,departShift
    except Exception:
        return None, None

def combineCountWeight(x, y):
    num01 = x[0] + y[0]
    num02 = x[1] + y[1]
    return (num01, num02)

def convertLineToJson(line):
    try:
        res={}
        res.update({"boardNum":line[0][0]})
        res.update({"planNum":line[0][1]})
        res.update({"destGateHub":line[0][2]})
        res.update({"date":line[0][3]})
        res.update({"batch":line[0][4]})
        res.update({"waybillCount":line[1][0]})
        res.update({"waybillWeight":line[1][1]})
        return [res]
    except Exception:
        return [None]

























