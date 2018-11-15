# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd
import json


def probabilityGenerateData(probabilityDistrubite, generateNum):
    """
        根据特征值的概率分布生成对应的数据
    :param probabilityDistrubite:特征值对应的概率分布
    :param num: 生成数据的数量
    :return: 通过概率模拟生成的数据
    """
    keys = probabilityDistrubite.keys()
    values = probabilityDistrubite.values()

    value_total = sum(values)
    arr_key = np.array(list(keys))
    # 元素数目向上取整,避免随机索引获取数据时出现数据越界
    arr_num = (np.ceil((np.array(list(values)) / value_total) * generateNum)).astype(dtype=np.int32)

    arr_list = arr_key.repeat(arr_num)
    arr_index = np.random.randint(0, generateNum, generateNum)

    arr_res = arr_list[arr_index]

    return arr_res


def normalDistributionGenerateData(loc, scale, size):
    """
        连续变量通过正态分布的方式生成数据
    :param loc: 期望值
    :param scale: 方差
    :param size: 生成数据的数据量
    :return: 通过正态分布模拟生成的数据
    """
    arr_res = np.random.normal(loc, scale, size)

    return arr_res


def subdivisionTask(dataDistrubite):
    """{"jobId": "0001",
          "lineCode":"010-020",
          "transType":"conveyance",
          "dataNum": 1000,
          "featureList": [
            {"weight": {"1.0": 0.3, "2.0": 0.3, "3.0": 0.4}},
            {"limitTypeCode": {"t4": 0.4, "t6": 0.3, "t801": 0.3}}
          ]
          }
        sparkStreaming通过Kafka实时获取数据生成任务,对数据生成任务进行解析,任务解析可以根据后续需求进行扩展
    :param dataDistrubite:数据生成任务json串,数据格式如上所示
    :return:返回一个数据任务Tuple,key为数据生成的数量,value为对应的数据特征列表
    """
    dataNum = dataDistrubite.pop("dataNum")
    featureList = dataDistrubite.pop("featureList")

    return dataDistrubite, dataNum, featureList


def aggregationFetureList(task):
    """
        获取数据生成任务tuple,根据数据生成数量和对应的数据特征列表,分别生成对应的特征数据,对生成数据进行聚合,返回聚合后的数据
    :param task:数据生成任务tuple
    :return:返回tuple,包括字段名称列表,聚合后的生成数据
    """
    dataDistrubite = task[0]
    dataNum = task[1]
    featureList = task[2]

    baseFeatureKeys = list(dataDistrubite.keys())

    keys = baseFeatureKeys
    arrays = []

    for feature in baseFeatureKeys:
        featureValue = dataDistrubite.get(feature)
        arr = np.repeat(np.array([featureValue]), dataNum)
        arrays.append(arr)

    for feature in featureList:
        key = list(feature.keys())[0]
        value = list(feature.values())[0]

        if value:
            arr = probabilityGenerateData(value, dataNum)
        else:
            arr = np.repeat(np.array([""]), dataNum)

        keys.append(key)
        arrays.append(arr)

    arrayData = np.vstack(tuple(arrays))

    return keys, arrayData


def convertAggregationData(aggregationData):
    """
        将聚合数据转换为json格式列表集合
    :param aggregationData:聚合数据tuple,包括字段列表,聚合数据
    :return:json格式的聚合列表
    """
    items = aggregationData[0]
    arrayData = aggregationData[1]

    df = pd.DataFrame(arrayData.T, columns=items)
    jsonData = json.loads(df.to_json(orient='records'))

    return jsonData
