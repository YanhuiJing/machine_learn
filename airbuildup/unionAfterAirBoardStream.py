#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from utils import isNotEmpty, isNotEmptyList, isEmptyList, \
    loadJson, getTimestampFromValue
from datetime import datetime as dt
from datetime import timedelta

from config import conf


def updateStateAfterBoardStream(airbuildupDStream):
    airBoardStream = airbuildupDStream.filter(isNotEmpty). \
        map(lambda line: line[1].decode("UTF8", "ignore")). \
        filter(isNotEmpty). \
        filter(lambda line: line.__contains__("701001")). \
        filter(lambda line: line.__contains__('\"vehicle_type\":\"1\"')). \
        map(getJson). \
        filter(isNotEmpty). \
        map(loadJson). \
        filter(isNotEmptyList). \
        flatMap(lambda line: line). \
        filter(lambda line: line.get("event_id") == "701001"). \
        map(convertDict). \
        filter(lambda line: line.get('vehicle_type') == "1"). \
        filter(lambda line: line.get('zone_code') == '755R'). \
        map(selectBuildUpFields). \
        map(convertDictToTuple). \
        updateStateByKey(accumulator). \
        checkpoint(conf.get("CHECKPOINT_INTERVAL", 1000))
    airBoardStream.count().map(lambda line: "The count of afterBoardState is %s" % line).pprint()
    afterAirBoard = airBoardStream.map(lambda line: line[1]). \
        map(convertLineToJson)

    afterAirBoard.repartition(1).saveAsTextFiles("/root/fengchi/airbuildup/afterBoardRes/json")
    return afterAirBoard


def getJson(line):
    try:
        start = line.index("[{\"app_key\"")
        end = line.index(",\"s_h\"")
        return line[start:end]
    except Exception:
        return None


def convertDict(line):
    pro = line.pop("properties")
    line.update(pro)
    time = line.get("time")
    timestamp = dt.fromtimestamp(int(float(time) / 1000.0))
    boardDate, boardShift = splitDateAndTime(timestamp)
    line["timestamp"] = timestamp
    line["boardDate"] = boardDate
    line["boardShift"] = boardShift
    return line


def splitDateAndTime(departTime):
    try:
        departDate = departTime.strftime("%Y-%m-%d")
        departHour = int(departTime.strftime("%H"))
        departMinute = int(departTime.strftime("%M"))
        shift_time = 5
        shift_num = 60 / shift_time
        shift_minute = int(departMinute / shift_time)
        if departHour < 12:
            date_temp = dt.strptime(departDate, "%Y-%m-%d") - timedelta(days=1)
            departDate = dt.strftime(date_temp, "%Y-%m-%d")
            departShift = (departHour + 12) * shift_num + shift_minute
        else:
            departShift = (departHour - 12) * shift_num + shift_minute
        return departDate, departShift
    except Exception:
        return None, None


"""
    获取打板完成后板的相关信息
"""


def selectBuildUpFields(line):
    try:
        return {
            "airBoardNum": line.get('contnr_code'),
            "destGatewayHub": line.get('dest_dept_code'),
            "waybillNum": line.get('user_def2'),
            "packageNum": line.get('user_def3'),
            "boardWeight": line.get('user_def4'),
            "timestamp": line.get("timestamp"),
            "boardDate": line.get("boardDate"),
            "boardShift": line.get("boardShift")
        }
    except Exception:
        return None


def convertDictToTuple(line):
    try:
        return (line.get('airBoardNum', None), line)
    except Exception:
        return None


def accumulator(newValue, lastValue):
    if isNotEmptyList(newValue):
        return newValue[0]
    elif isEmptyList(newValue) and isRecordsExpired(lastValue):
        return None
    else:
        return lastValue


def isRecordsExpired(oldValue):
    expireDays = conf.get("PACKAGE_EXPIRE_DAYS", 1)

    return getTimestampFromValue(oldValue) < (dt.now() - timedelta(days=expireDays))


def convertLineToJson(line):
    try:
        res = {}
        res.update({"boardNum": line.get("airBoardNum")})
        res.update({"destGateHub": line.get("destGatewayHub")})
        res.update({"date": line.get("boardDate")})
        res.update({"batch": line.get("boardShift")})
        res.update({"waybillCount": line.get("waybillNum")})
        res.update({"packageCount": line.get("packageNum")})
        res.update({"boardWeight": line.get("boardWeight")})
        return res
    except Exception:
        return None
