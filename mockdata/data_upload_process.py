# -*- coding: UTF-8 -*-

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from vector_mock_data import subdivisionTask, aggregationFetureList, convertAggregationData
from data_conversion import generateTime, generateZoneCode, generateWayBillNo, convertTimeLine, generateWaybillInfoList, \
    convertDataInfoTotuple, convertRelatedInfo
# ,convertDataInfoTotuple,convertRelatedInfo
import time
from datetime import datetime
from util.utilities import loadJson, log, isNotEmpty

from kafka import KafkaProducer

import json

from data_conversion import *

logger = log.getLogger()


def getDStream():
    conf = SparkConf("num_reduce")
    sc = SparkContext(conf=conf)

    start = time.time()

    processDStream(sc)

    end = time.time()

    print("cost time is %s" % (end - start))

    return sc

    """
        创建sparkStreamingContext对象,通过sparkContext()设置系统参数,对实时处理程序进行调优
    """


def processDStream(dStream):
    def isNotEmptyTime(line):
        if line:
            return True
        else:
            return False

    def timeList():
        timeLine = ("2018-10-19:22:30:0", 0)
        return [timeLine, ]

    def convertTuple(tuple):
        key = tuple[0]
        value = tuple[1]

        key = int(float(key.split(":")[1]))

        return (key, value)

    rdd01 = dStream.textFile("./result.txt"). \
        map(loadJson). \
        flatMap(generateWaybillInfoList). \
        map(convertDataInfoTotuple). \
        map(convertTuple)

    subTime = timeList()
    rdd02 = dStream.parallelize(subTime). \
        map(convertTimeLine)

    rdd02.join(rdd01). \
        map(convertRelatedInfo). \
        map(subdivisionTask). \
        map(aggregationFetureList). \
        map(convertAggregationData). \
        flatMap(lambda line: line). \
        map(generateTime). \
        map(generateZoneCode). \
        map(generateWayBillNo). \
        foreach(print)


if __name__ == '__main__':
    getDStream()

