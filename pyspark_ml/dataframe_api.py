# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession


def get_sparksession():
    spark = SparkSession.builder. \
        master("local"). \
        appName("dataframe"). \
        getOrCreate()

    return spark


if __name__ == '__main__':
    spark = get_sparksession()

    spark.range(0,100,10).show()


