#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
from datetime import datetime as dt
from datetime import timedelta
from utils import isNotEmpty,isNotEmptyList,getTimestampFromValue,\
           timestampToDatetime,isEmpty, stringToDatetime,\
           isLoadGoodsOperation,isUnlockVehicleOperation,isAirRelatedOperation

from config import conf

"""
    聚合装车流,封车流,解封车流,运单流,打板流
        1,数据添加:装车流、运单流添加状态机数据
        2,数据更新:根据不同的操作,更新字段数据
        3,数据删除:打板操作或者数据过期删除数据
"""


def updateStateBeforeBoardStream(bargunVehicleStream, vehicleStream,waybillStream, airBoardStream):


    unionStream = bargunVehicleStream.union(waybillStream).union(vehicleStream).union(airBoardStream)

    packageSgtate = unionStream.filter(isNotEmpty). \
                            updateStateByKey(accumulator). \
                            map(lambda kvs: kvs[1])

    packageSgtate.checkpoint(conf.get("CHECKPOINT_INTERVAL", 1000))


    unionResult = packageSgtate.filter(hasArriveTime). \
                            filter(hasGateway). \
                            map(changePackageToCombinedKeys). \
                            reduceByKey(combineCountWeight). \
                            map(convertTupleToJson)


    return unionResult.reduce(lambda x,y:x+y)


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
    打板前操作,根据装车操作与运单数据添加新数据,其他操作直接返回None值进行过滤
"""
def addNewPackage(newValue):

    lastData = {}
    for value in newValue:
        inputData = value

        if isLoadGoodsOperation(inputData):
            lastData = updateOldPackageByLoadGoods(inputData, lastData)
        elif isWaybillOperation(inputData):
            lastData = updateOldPackageByWaybill(inputData, lastData)
        else:
            lastData=None

    return lastData


def updateOldPackage(newValue, oldValue):
    lastData = oldValue
    for value in newValue:
        inputData = value

        if isLoadGoodsOperation(inputData):
            lastData = updateOldPackageByLoadGoods(inputData, lastData)
        elif isUnlockVehicleOperation(inputData):
            lastData = updateOldPackageByUnLockVehicles(inputData, lastData)
        elif isAirRelatedOperation(inputData):
            return None
        elif isWaybillOperation(inputData):
            lastData = updateOldPackageByWaybill(inputData, lastData)
    return lastData


"""
    根据不同的操作类型更新数据
"""

def updateOldPackageByLoadGoods(newValue, oldValue):
    try:
        oldValue.update({'timestamp': newValue.get('timestamp', None)})
        oldValue.update({'opCode': newValue.get('opCode', None)})
        oldValue.update({'barScanTm': newValue.get('barScanTm', None)})
        oldValue.update({'planRunTime': newValue.get('planRunTime', None)})
        oldValue.update({'arriveTime': newValue.get('arriveTime', None)})
        return oldValue
    except Exception:
        return oldValue


def updateOldPackageByUnLockVehicles(newValue, oldValue):
    try:
        barScanTm = newValue.get('barScanTm', None)
        arriveTime = timestampToDatetime(barScanTm)

        oldValue.update({'timestamp': newValue.get('timestamp', None)})
        oldValue.update({'opCode': newValue.get('opCode', None)})
        oldValue.update({'barScanTm': newValue.get('barScanTm', None)})
        oldValue.update({'zoneCode': newValue.get('zoneCode', None)})
        oldValue.update({'arriveTime': arriveTime})
        return oldValue

    except Exception:
        return oldValue


def updateOldPackageByWaybill(newValue, oldValue):
    try:
        oldValue.update({'timestamp': newValue.get('timestamp', None)})
        oldValue.update({'waybillNo': newValue.get('waybillNo', None)})
        oldValue.update({'waybillId': newValue.get('waybillId', None)})
        oldValue.update({'consignedTm': newValue.get('consignedTm', None)})
        oldValue.update({'meterageWeightQty': newValue.get('meterageWeightQty', None)})
        oldValue.update({'realWeightQty': newValue.get('realWeightQty', None)})
        oldValue.update({'destZoneCode': newValue.get('destZoneCode', None)})
        oldValue.update({'sourceZoneCode': newValue.get('sourceZoneCode', None)})
        oldValue.update({'limitTypeCode': newValue.get('limitTypeCode', None)})
        oldValue.update({'destGatewayHub': newValue.get('destGatewayHub', None)})
        return oldValue
    except Exception:
        return oldValue


def isWaybillOperation(dict):
    if isNotEmpty(dict.get('waybillId', None)):
        return True
    else:
        return False


def hasArriveTime(line):
    try:
        flag=False
        arriveTime = line.get("arriveTime")
        if isNotEmpty(arriveTime):
            flag=True
        return flag
    except Exception:
        return False


def hasGateway(line):
    try:
        flag=False
        destGatewayHub = line.get("destGatewayHub")
        if isNotEmpty(destGatewayHub):
            flag=True
        return flag
    except Exception:
        return False


# def isAirTransformType(line):
#     try:
#         res = conf.get("AIR_TRANSFORM_TYPE", "T4,T6,T8")
#         isAirType = (line.get("limitTypeCode") in res)
#         return isAirType
#     except Exception:
#         return False

def changePackageToCombinedKeys(value):
    try:
        arriveTime = value.get('arriveTime', None)
        departDate, shift = calculateDepartShiftAndDate(arriveTime)
        destGatewayHub=value.get('destGatewayHub', None)

        realWeightQty = float(value.get('realWeightQty', None))

        return ((departDate,shift,destGatewayHub), (1, realWeightQty))
    except Exception as err:
        return None, err


def calculateDepartShiftAndDate(planDepartTime):
    try:

        departDate, departShift = splitDateAndTime(planDepartTime)

        return departDate, departShift
    except Exception as error:
        return None, None


"""
   快件预计到达时间,配载航线预计出发时间按照日期与时间段进行分隔

"""


def splitDateAndTime(departTime):
    try:
        departDate=departTime.strftime("%Y-%m-%d")
        departHour=departTime.strftime("%H")
        departMinute=departTime.strftime("%M")
        shift_time = 5
        shift_num = 60 / shift_time
        shift_minute=int(departMinute / shift_time)
        if departHour < 12:
            date_temp = dt.strptime(departDate, "%Y-%m-%d") - timedelta(days=1)
            departDate =dt.strftime(date_temp, "%Y-%m-%d")
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


def convertTupleToJson(line):
    try:
        result = {}
        result["date"] = line[0][0]
        result["batch"] = line[0][1]
        result["destGateHub"] = line[0][2]
        result["waybillCount"] = line[1][0]
        result["waybillWeight"] = line[1][1]

        return [result]
    except Exception:

        return [None]





