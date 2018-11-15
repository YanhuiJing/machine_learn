# -*- coding: UTF-8 -*-

import hashlib
import json
from datetime import datetime
from datetime import timedelta
import random
import math
from simulation.mockdata.config import config
from simulation.util.utilities import stringToBytes
from kafka import KafkaProducer

"""
    数据转换逻辑
    todo=>1:快件重量为整数,生成对应的随机小数
          2:根据快件重量生成对应的托寄物
"""


def generateWaybillInfoList(waybillInfo):
    """
        依据运单信息的时间分布,依据hour将一条数据转换为多条数据
    :param waybillInfo:运单信息
    :return:list,转换后的运单列表
    """
    airFlyTime = waybillInfo.pop("airFlyTm")

    total = sum(airFlyTime.values())

    waybillList = []
    for item in airFlyTime.keys():
        newwaybillinfo = waybillInfo.copy()
        probablity = round(airFlyTime.get(item) / total, 3)
        newwaybillinfo.update({"airFlyTm": (item, probablity)})
        waybillList.append(newwaybillinfo)

    return waybillList


def convertDataInfoTotuple(waybillInfo):
    """
        将运单信息转换为元祖格式,与时间轴信息进行关联
    :param waybillInfo:运单信息
    :return:tuple,key为小时,value为运单信息
    """
    lineCode = waybillInfo.get("lineCode")
    hour = int(float(waybillInfo.get("airFlyTm")[0]))

    key = lineCode + ":" + str(hour)

    return key, waybillInfo


def generateTime(waybillInfo):
    """
        获取运单时间生成逻辑,将生成时间添加到运单信息中返回
    :param waybillInfo: 运单信息
    :return: 返回修改后的运单信息
    """
    timeinfo = waybillInfo.get("airFlyTm").split(":")

    date = datetime.strptime(timeinfo[0], "%Y-%m-%d")
    hourDelta = timedelta(hours=int(timeinfo[1]))
    mircoSecondDelta = timedelta(milliseconds=random.randint(int(timeinfo[2]), int(timeinfo[3])))
    randomDate = (date + hourDelta + mircoSecondDelta)
    airFlyTm = randomDate.strftime("%Y-%m-%d %H:%M:%S")
    nextNodeTm = randomDate.timestamp()
    waybillInfo.update({"airFlyTm": airFlyTm})
    waybillInfo.update({"nextNodeTm": nextNodeTm})

    # 动态添加下一节点主题 todo
    waybillInfo.update({"nextNode": "pre"})
    return waybillInfo


def generateZoneCode(waybillInfo):
    """
        通过lineCode获取运单发出地,运单目的地
    :param waybillInfo:运单信息
    :return:返回添加发出地,目的地后的运单信息
    """
    lineCode = waybillInfo.get("lineCode")
    srcAirCode = lineCode.split("-")[0]
    destAirCode = lineCode.split("-")[1]
    srcZoneCode = lineCode.split("-")[2]
    destZoneCode = lineCode.split("-")[3]

    waybillInfo.update({"srcAirCode": srcAirCode})
    waybillInfo.update({"destAirCode": destAirCode})
    waybillInfo.update({"srcZoneCode": srcZoneCode})
    waybillInfo.update({"destZoneCode": destZoneCode})

    return waybillInfo


"""
    json时间解析类,datetime解析为字符串时自动调用该类
"""


class MyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime,)):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return super().default(obj)


def generateWayBillNo(waybillInfo):
    """
        基于数据仿真模块生成的运单信息,通过md5加密的方式生成运单号
    :param waybillInfo:数据仿真模块生成的运单信息
    :return:返回添加运单号后的运单信息
    """
    md5 = hashlib.md5()
    md5.update(json.dumps(waybillInfo, cls=MyJSONEncoder).encode("utf-8"))
    waybillno = md5.hexdigest()[:15]
    waybillInfo.update({"waybillNo": waybillno})
    return waybillInfo


def isTimeLine(metainfo):
    """
        从元数据主题信息中,过滤获取时间轴信息
    :param metainfo: 从kafka主题中获取的元数据信息
    :return: 时间轴信息返回True,否则返回False
    """

    return True if metainfo[0] == "timer" else False


def convertTimeLineToBatch(timeLine):
    """
        将时间轴信息按照数据生成的时间批次转换为对应的时间批次信息
    :param timeLine: 时间轴信息
    :return: tuple,key为时间批次信息,value为1
    """
    timeInfo = json.loads(timeLine[1])

    startTime = datetime.fromtimestamp(timeInfo.get("curTime"))

    date = str(startTime.date())
    hour = str(startTime.hour)
    interval = 30
    minBatch = startTime.minute

    batch = str(int(minBatch / interval))

    keyTimeInfo = ":".join([date, hour, str(interval), batch])

    return keyTimeInfo, 1


def isEffectiveTimeLine(timeLine):
    """
        根据时间轴信息累加次数判断是否发送数据
    :return:  0为首次出现返回True,否则返回False
    """
    return True if timeLine[1] == 0 else False


def convertTimeLine(timeLine):
    """
        转换时间轴信息,依据hour与运单信息进行关联
    :param timeLine: 时间轴信息
    :return: tuple,key为hour,value为转换后的时间轴信息
    """

    timeinfo = timeLine[0]

    timeinfoList = timeinfo.split(":")

    batch = int(timeinfoList.pop(3))
    interval = int(timeinfoList.pop(2))
    hour = int(timeinfoList[1])

    start = str(interval * 60 * 1000 * batch)
    end = str(interval * 60 * 1000 * (batch + 1))

    timeinfoList.append(start)
    timeinfoList.append(end)

    timeinfos = ":".join(timeinfoList)

    return hour, (timeinfos, interval)


def convertRelatedInfo(relatedInfo):
    """
        对时间轴与运单信息关联后的数据进行解析,计算时间轴对应的数据生成数目,将时间轴信息添加到运单信息中
    :param relatedInfo: 时间轴与运单关联信息
    :return: dict，更新后的运单信息
    """
    timeinfo = relatedInfo[1][0]
    waybillinfo = relatedInfo[1][1]

    dataNum = waybillinfo.pop("dataNum")
    airFlyTm = waybillinfo.pop("airFlyTm")

    dataNum = math.ceil(dataNum * airFlyTm[1] * (timeinfo[1] / 60))

    waybillinfo.update({"dataNum": dataNum})
    waybillinfo.update({"airFlyTm": timeinfo[0]})

    return waybillinfo


def sendMessageByKafka(waybillinfos):
    """
        将生成的运单信息,通过foreachPartition依次发出
    :param waybillinfos:每个partition的信息列表
    """
    brokers = config.get("KAFKA_BROKERS")
    producer = KafkaProducer(bootstrap_servers=brokers, retries=10, api_version_auto_timeout_ms=60000)
    topic = config.get("WAYBILL_TOPIC")

    for waybillinfo in waybillinfos:
        key = stringToBytes(waybillinfo.get("waybillNo"))
        value = stringToBytes(json.dumps(waybillinfo))
        producer.send(topic, key=key, value=value)

    producer.flush()

    producer.close()
