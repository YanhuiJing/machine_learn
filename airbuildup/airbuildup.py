#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from __future__ import print_function

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition


from airBargun import convertToBargunState
from airwaybill import parseWaybill

from unionBeforeAirBoardStream import updateStateBeforeBoardStream
from unionAirBoardingStream import updateStateAirBoardingStream
from unionAfterAirBoardStream import updateStateAfterBoardStream

from utils import getStatusByWindow, \
    isDevMode, isEmpty, isNotEmpty, \
    postRequest, saveAsText, spotDecoder, isNotEmptyList, \
    timestampToDatetime, getTimestampFromValue, \
    loadJson, loadValueFromKafka, valueFromListToElement, sendStatus

import requests
import json

import log
from datetime import datetime as dt
from datetime import timedelta

from config import conf

# Codec
UTF8 = 'utf-8'

# Set logger
logger = log.getLogger()


def createNewContext():
    ssc, dataSource = getSparkStreamAll()
    processDStream(dataSource)
    return ssc

def createStreamingContext():
    _sparkConf = SparkConf(). \
        set("spark.streaming.backpressure.enabled", True). \
        set("spark.streaming.kafka.maxRatePerPartition", conf.get("MAX_RATE_PER_PARTITION", 1000)). \
        set("spark.default.parallelism", conf.get("TOTAL_CORES", 2)). \
        set("spark.locality.wait", "500ms"). \
        set("spark.streaming.blockInterval", "1s")
    sc = SparkContext(appName=conf.get("APP_NAME", "airbuildup"), conf=_sparkConf)
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, conf.get("BATCH_INTERVAL_SECONDS", 10))

    checkpointPath = conf.get("CHECKPOINT_PATH")
    ssc.checkpoint(checkpointPath)
    return ssc

def getSparkStreamAll():
    ssc = createStreamingContext()
    dataSourceList = conf.get("DATA_SOURCE_LIST")

    dStream = {}

    for dataSource in dataSourceList:
        source = conf.get(dataSource)
        brokers = source.get('broker')
        topic = source.get('topic')
        groupId = conf.get("GROUP_ID")
        offset = conf.get("OFFSET_MODE")

        stream = getSparkStream(ssc, brokers, topic, groupId, offset)
        dStream.update({dataSource: stream})

    mockStream = ssc.textFileStream(conf.get("ZONE_HUB_DIR"))
    dStream.update({"zoneHub": mockStream})

    return ssc, dStream


def getSparkStream(ssc, brokers, topic, groupId, offsetMode):
    kafkaParams = {"metadata.broker.list": brokers, "group.id": groupId, "auto.offset.reset": offsetMode}

    logger.info("Creating direct stream from kafka with ssc %s, topic %s, params %s" % (ssc, topic, kafkaParams))
    directDStream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams, keyDecoder=spotDecoder,
                                                  valueDecoder=spotDecoder)

    return directDStream


"""
    数据流的核心处理逻辑
"""
def processDStream(dataSource):
    conveyanceDStream = getDataFromKafka(dataSource.get('conveyance'))
    bargunDStream = getDataFromKafka(dataSource.get('bargun'))
    waybillDStream = getDataFromKafka(dataSource.get('waybill'))
    afterAirBoardDStream = getDataFromKafka(dataSource.get('inc_ubas'))
    zoneHubDStream = getDataFromKafka(dataSource.get("zoneHub"))

    """
        过滤把枪流,过滤条件:1,深圳本地发出;2,装货,封车,解封车操作（通过车标与运力流进行关联）
    """
    loadGoodsStream, vehicleStream, airRelatedStream, airFlyStream = convertToBargunState(bargunDStream)
    # vechileStream.repartition(1).saveAsTextFiles("hdfs://10.117.110.204:9000/root/fengchi/transformer/checkpoint/bargun/data")

    """
        运力流,发出城市为深圳,运力目的地代码为755R
    """
    conveyanceState = convertToConveyanceState(conveyanceDStream)

    """ 
        包含车标的把枪流与运力流通过车标信息进行关联
    """
    bargunVehicleStream = intersectVechileWithConveyance(loadGoodsStream, conveyanceState)

    """
        运单流,过滤条件:1,本地发出;2,目的地为广东省外;3,运单时效为T4-T8
    """
    waybillStream = parseWaybill(waybillDStream, zoneHubDStream)


    beforeAirBoardRes = updateStateBeforeBoardStream(bargunVehicleStream, vehicleStream, waybillStream,
                                                     airRelatedStream)

    airBoardingRes = updateStateAirBoardingStream(waybillStream, airRelatedStream, airFlyStream)

    afterAirBoardRes = updateStateAfterBoardStream(afterAirBoardDStream)

    postWindowSize = conf.get("WINDOW_SIZE", 100)
    beforeBoardPackageStatusByWindow = getStatusByWindow(beforeAirBoardRes, postWindowSize, postWindowSize)

    airBoardingPackageStatusByWindow = getStatusByWindow(airBoardingRes, postWindowSize, postWindowSize)

    afterBoardPackageStatusByWindow = getStatusByWindow(afterAirBoardRes, postWindowSize, postWindowSize)

    sendBeforeBoard(beforeBoardPackageStatusByWindow.repartition(1), getRequestUrl())

    sendBoarding(airBoardingPackageStatusByWindow.repartition(1), getRequestUrl())

    sendAfterBoard(afterBoardPackageStatusByWindow.repartition(1), getRequestUrl())


def getDataFromKafka(dStream):
    return dStream.filter(isNotEmpty)


"""
    运力流数据处理流程：
        1,过滤从深圳本地发出,到达755R的数据
        2,选取字段=>线路编码,计划出发时间,计划到达时间
        3,数据转换=>转换为（线路编码,计划行驶时间）tuple格式
        4,以运力线路编码作为key,对运力的状态机进行更新
"""


def convertToConveyanceState(conveyanceDStream):
    conveyanceDStream = conveyanceDStream.map(loadValueFromKafka). \
        filter(isNotEmpty). \
        filter(isTo755R). \
        map(selectConveyanceFields). \
        map(constructConveyanceKVpair). \
        updateStateByKey(accumulatorConveyance). \
        map(valueFromListToElement)

    conveyanceDStream.checkpoint(conf.get("CHECKPOINT_INTERVAL", 100))
    return conveyanceDStream


def isTo755R(dict):
    flag = False
    linecode = dict.get('line_code')
    if "755R" in linecode[3:]:
        flag = True
    return flag


"""
     line_code=>线路编码
     'plan_depart_tm':计划出发时间
     'plan_arrive_tm':计划到达时间
"""


def selectConveyanceFields(dict):
    try:
        return {
            'line_code': dict.get('line_code'),
            'plan_depart_tm': dict.get('planDepartTm'),
            'plan_arrive_tm': dict.get('planArriveTm')
        }
    except Exception:
        return None


def constructConveyanceKVpair(dict):
    try:
        line_code = dict.get('line_code')
        plan_depart_tm = dict.get('plan_depart_tm')
        plan_arrive_tm = dict.get('plan_arrive_tm')
        plan_run_tm = timestampToDatetime(plan_arrive_tm) - timestampToDatetime(plan_depart_tm)
        return (line_code, plan_run_tm)
    except Exception:
        return None


def accumulatorConveyance(newValue, lastValue):
    if isNotEmptyList(newValue):
        return newValue
    else:
        return lastValue


"""
    装车把枪流与运力流关联：
        1,通过线路编码作为key进行关联
        2,计算预计到达时间:装车把枪扫描时间+预计行驶时间+装车操作预计时间间隔
"""


def intersectVechileWithConveyance(vechileStream, conveyanceDStream):
    def preArriveTime(dict):
        barScaninfo = dict[1][0]
        waybillno = barScaninfo.get("waybillNo")
        barScanTm = barScaninfo.get("barScanTm")
        planRunTime = dict[1][1]
        arriveTime = timestampToDatetime(barScanTm) + planRunTime

        barScaninfo.update({"arriveTime": arriveTime, "planRunTime": planRunTime})

        return (waybillno, barScaninfo)

    bargunVehicleDStream = vechileStream.join(conveyanceDStream).map(preArriveTime)

    return bargunVehicleDStream


def getStatusByWindow(result, windowDuration, slideDuration):
    return result.reduceByWindow(lambda x, y: y, None, windowDuration, slideDuration)


def _formatRow(record):
    return record


def _getParcels(partition):
    parcels = []
    for record in partition:
        parcels.append(_formatRow(record))
    return parcels


def sendBeforeBoard(result, url):
    def getBeforeBoard(partition):
        res = {}
        parcels = _getParcels(partition)
        res.update({"GatewayHub": "755R"})
        res.update({"boardAction": "before"})
        res.update({"Parcels": parcels})
        postRequest(url, json.dumps(res))

    result.foreachRDD(lambda rdd: rdd.foreachPartition(getBeforeBoard))


def sendBoarding(result, url):
    def getBoarding(partition):
        res = {}
        parcels = _getParcels(partition)
        res.update({"GatewayHub": "755R"})
        res.update({"boardAction": "boarding"})
        res.update({"Parcels": parcels})
        postRequest(url, json.dumps(res))

    result.foreachRDD(lambda rdd: rdd.foreachPartition(getBoarding))


def sendAfterBoard(result, url):
    def getAfterBoard(partition):
        res = {}
        parcels = _getParcels(partition)
        res.update({"GatewayHub": "755R"})
        res.update({"boardAction": "after"})
        res.update({"Parcels": parcels})
        postRequest(url, json.dumps(res))

    result.foreachRDD(lambda rdd: rdd.foreachPartition(getAfterBoard))


def getRequestUrl():
    url = conf.get("OUTPUT_REQUEST_URL", "http://localhost:8214")
    return url


if __name__ == '__main__':

    try:
        sendStatus(200, '0')
        sendStatus(int(dt.now().timestamp()), '1')

        if isDevMode(conf):
            ssc = createNewContext()
        else:
            ssc = StreamingContext.getOrCreate(conf.get("CHECKPOINT_PATH"), createNewContext)

        executionTime = conf.get("EXECUTION_TIME", -1)
        if executionTime <= 0:
            ssc.start()
            ssc.awaitTermination()
        else:
            ssc.awaitTerminationOrTimeout(executionTime)

            logger.info("Stoping process dStream gracefully...")

            ssc.stop(True, True)
    finally:
        sendStatus(400, '0')
