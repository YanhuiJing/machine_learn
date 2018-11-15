#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pyspark import SparkConf
from pyspark import SparkContext

from datetime import datetime as dt
from datetime import timedelta


import json

if __name__ == '__main__':
    conf=SparkConf("num_reduce")
    sc=SparkContext(conf=conf)


    """
        755R实时装车数量计算
    """
    #
    #
    # def replaceContent(line):
    #     res = line.replace("(", "").\
    #             replace(")", "").\
    #             replace("\'", "").\
    #             replace("{","").\
    #             replace("}","").\
    #             split(",")
    #     dict={}
    #     for data in res:
    #         if data.__contains__(":"):
    #             split_res=data.split(":")
    #             dict.update({split_res[0].strip():split_res[1].strip()})
    #     dict_res={}
    #     dict_res["waybillNo"]=dict.get("waybillNo")
    #     dict_res["barScanDate"]=dt.fromtimestamp(int(float(dict.get("barScanTm")) / 1000.0)).date().strftime("%Y-%m-%d")
    #     dict_res["barScanHour"]=dt.fromtimestamp(int(float(dict.get("barScanTm")) / 1000.0)).time().hour
    #     dict_res["lineCode"]=dict.get("lineCode")
    #
    #     return (dict_res.get("barScanDate"),dict_res.get("waybillNo"))
    #
    # num=sc.textFile("/Users/gavin/Downloads/part-00000"). \
    #     map(replaceContent).\
    #     distinct().\
    #     map(lambda line:(line[0],1)).\
    #     reduceByKey(lambda x, y: x + y). \
    #     foreach(print)

    """
        实时发车,解封车时间预处理
    """

    # def loadjson(line):
    #     try:
    #         return json.loads(line)
    #     except Exception:
    #         pass
    #
    # def convertTime(line):
    #     try:
    #         barScanTm=line.get("barScanTm")
    #         time=dt.fromtimestamp(int(float(barScanTm)/1000.0))
    #         line["time"]=time
    #         return line
    #     except Exception:
    #         pass
    #
    #
    # rdd1=sc.textFile("/Users/gavin/Downloads/part-00000").\
    #     map(lambda line:line.replace("\'", "\"")).\
    #     map(loadjson).\
    #     map(convertTime).\
    #     map(lambda line:((line.get("waybillno"),line.get("opcode"),line.get("zoneCode")),line.get("time"))).\
    #     groupByKey().\
    #     map(lambda line:(line[0],list(line[1])[0])).\
    #     cache()
    #
    # rdd1.filter(lambda line:line[0][1]=='36').\
    #     map(lambda line:line[0][0]+","+line[1].strftime("%Y-%m-%d %H:%M:%S")).\
    #     repartition(1).saveAsTextFile("/Users/gavin/Downloads/36")
    #
    # rdd1.filter(lambda line: line[0][1] == '37'). \
    #     filter(lambda line: line[0][2] == '755R'). \
    #     map(lambda line: line[0][0]+","+line[1].strftime("%Y-%m-%d %H:%M:%S")). \
    #     repartition(1).saveAsTextFile("/Users/gavin/Downloads/37")


    """
        实时装车时间预处理
    """


    # def loadjson(line):
    #     try:
    #         return json.loads(line)
    #     except Exception:
    #         pass
    #
    #
    # def convertTime(line):
    #     try:
    #         barScanTm=line.get("barScanTm")
    #         time=dt.fromtimestamp(int(float(barScanTm)/1000.0))
    #
    #         planRunTime=line.get("planRunTime")
    #         arriveTime=(time+timedelta(seconds=planRunTime)).strftime("%Y-%m-%d %H:%M:%S")
    #
    #         line["arriveTime"]=arriveTime
    #         return line
    #     except Exception:
    #         pass
    #
    # rdd=sc.textFile("/Users/gavin/Downloads/loadGoods"). \
    #     map(lambda line: line.replace("\'", "\"")). \
    #     map(loadjson).\
    #     map(convertTime).\
    #     map(lambda line:(line.get("waybillno"),(line.get("planRunTime"),line.get("arriveTime")))).\
    #     groupByKey().\
    #     map(lambda line:(line[0],list(line[1])[0])).\
    #     cache()
    #
    # rdd.map(lambda line:line[0]+","+str(line[1][0])). \
    #     repartition(1).saveAsTextFile("/Users/gavin/Downloads/plantime")
    #
    # rdd.map(lambda line: line[0] + "," + line[1][1]). \
    #     repartition(1).saveAsTextFile("/Users/gavin/Downloads/30")



    """
        数据关联时间预测
    """
    # def convertTime(line):
    #     res=line.split(",")
    #     waybillno=res[0]
    #     time=dt.strptime(res[1],"%Y-%m-%d %H:%M:%S")
    #     return (waybillno,time)
    #
    # def convertPlan(line):
    #     res = line.split(",")
    #     waybillno = res[0]
    #     timeDiff =int(res[1])
    #
    #     return (waybillno,timeDiff)
    #
    # def convertTuple(line):
    #     # return (line**2,1)
    #     if abs(line)<600:
    #         return ("10-",1)
    #     elif abs(line)<1800:
    #         return ("10~30",1)
    #     elif abs(line)<3600:
    #         return ("30~60",1)
    #     else:
    #         return ("60+",1)
    #
    # def add(x,y):
    #     return (x[0]+y[0],x[1]+y[1])
    #
    # rdd30=sc.textFile("/Users/gavin/Downloads/30time").\
    #         map(convertTime)
    #
    #
    # rdd36=sc.textFile("/Users/gavin/Downloads/vehicle/part-00000"). \
    #         map(convertTime).\
    #         groupByKey().\
    #         map(lambda line:(line[0],list(line[1])[0]))
    #
    #
    # rdd37=sc.textFile("/Users/gavin/Downloads/37time"). \
    #         map(convertTime)
    #
    #
    # rddplan=sc.textFile("/Users/gavin/Downloads/conveyance_time").\
    #         map(convertPlan)
    #
    #
    #
    # rdd30diff=rdd30.join(rdd37).\
    #         map(lambda line:line[1][1].timestamp()-line[1][0].timestamp()-292).\
    #         map(convertTuple). \
    #         reduceByKey(lambda x, y: (x + y)).\
    #         take(10)
    #
    #
    # print(rdd30diff)
    #
    # rdd36diff=rddplan.join(rdd36).\
    #         map(lambda line:(line[0],line[1][1]+timedelta(seconds=line[1][0]))).\
    #         join(rdd37). \
    #         map(lambda line: line[1][1].timestamp() - line[1][0].timestamp()-94). \
    #         map(convertTuple). \
    #         reduceByKey(lambda x, y: (x + y)). \
    #         take(10)
    #
    # print(rdd36diff)

    """
        封车数据处理
    """
    # def convertTime(line):
    #     res=line.split(",")
    #     waybillno=res[0]
    #     time=res[1]
    #     zonecode=res[2]
    #     return (waybillno,time,zonecode)
    #
    # def filterZone(line):
    #     zonecode=line[2]
    #     if zonecode in "755W,755WE,755WF,755X,755GA,755VE":
    #         return True
    #     else:
    #         return False
    #
    # num=sc.textFile("/Users/gavin/Downloads/36time2"). \
    #         map(convertTime).\
    #         filter(filterZone).\
    #         map(lambda line:line[0]+","+line[1]+","+line[2]).\
    #         repartition(1).\
    #         saveAsTextFile("/Users/gavin/Downloads/vehicle")

    """
        to755R数据统计
    """

    res=sc.textFile("/Users/gavin/Downloads/loadGoodsTo755R").\
        map(lambda line:line.replace("\'", "\"")).\
        map(lambda line:json.loads(line)).\
        map(lambda line:line.get("barScanTm")).\
        map(lambda line:dt.fromtimestamp(int(float(line)/1000.0))).\
        map(lambda line:(line.strftime("%Y-%m-%d"),1)).\
        reduceByKey(lambda x,y:x+y).\
        collect()

    print(res)






































