#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pyspark.sql import SparkSession

spark=SparkSession.builder.\
                master("local").\
                appName("sparkSql_operator").\
                getOrCreate()


df=spark.read.csv("./data/customers.csv",header=True)


"""
    dataFrame分区repartition与coalesce区别
        1,是否进行shuffle,repatition操作有shuffle,coalesce操作没有shuffle
        2,分区数,repartition指任意分区数都能够执行,coalesce指定的分区数必须少于之前的分区数才能够执行
        3,repartition操作可以指定分区列
"""

# print(df.rdd.getNumPartitions())
# print(df.coalesce(3).rdd.getNumPartitions())
# print(df.repartition(3).rdd.getNumPartitions())

"""
    通过dataFrame创建临时表
        1,createGlobalTempView=>在sparkContext的生命周期都能够访问这个表
        2,createTempView=>只有在sparkSession的生命周期中才能访问该表
"""

# df.createGlobalTempView("global")
# spark.sql("select * from global_temp.global limit 10").show()

# df.createTempView("temp")
# spark.sql("select * from temp limit 10").show()

# df.createTempView("temp")
# spark.sql("select Age,Genre,count(*) as num from temp group by Age,Genre order by num desc,Age asc").show()

from pyspark.sql.functions import *

df.describe().show()

















spark.stop()