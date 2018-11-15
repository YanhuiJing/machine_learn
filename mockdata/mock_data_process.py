from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from simulation.mockdata.vector_mock_data import *
from simulation.mockdata.data_conversion import *

from simulation.util.utilities import loadJson
from simulation.util.log import Logger
from simulation.common import MOCK_DATA

from simulation.util.spark_streaming_utils import getStreamingContextAndStream

logger = Logger()


class MockData:

    def __init__(self, conf):
        self.conf = conf

    def createStreamingContext(self):
        """
           sparkStreaming参数设置
        """
        sparkConf = SparkConf(). \
            set("spark.streaming.backpressure.enabled", True). \
            set("spark.streaming.kafka.maxRatePerPartition", self.conf.get("MAX_RATE_PER_PARTITION", 1000)). \
            set("spark.default.parallelism", self.conf.get("TOTAL_CORES", 2)). \
            set("spark.locality.wait", "500ms"). \
            set("spark.streaming.blockInterval", "1s"). \
            set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.3.2")

        sc = SparkContext(appName=self.conf.get("APP_NAME", "mockdata"), conf=sparkConf)
        sc.setLogLevel("WARN")

        ssc = StreamingContext(sc, self.conf.get("BATCH_INTERVAL_SECONDS", 10))

        checkpointPath = self.conf.get("CHECK_POINT_PATH")
        ssc.checkpoint(checkpointPath)
        return ssc

    def getKafkaStream(self, ssc, brokers, topic, offsetMode):
        """
            创建连接kafka的directStream
        """
        kafkaParams = {"metadata.broker.list": brokers, "group.id": self.conf.get("GROUP_ID"),
                       "auto.offset.reset": offsetMode}

        logger.info("Creating direct stream from kafka with ssc %s, topic %s, params %s" % (ssc, topic, kafkaParams))
        directDStream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)

        return directDStream

    def getMockDataStream(self):
        """
            从kafka获取所有数据流
        """
        ssc = self.createStreamingContext()
        dataSourceList = self.conf.get("DATA_SOURCE_LIST")

        dStream = {}

        for dataSource in dataSourceList:
            source = self.conf.get(dataSource)
            brokers = source.get('broker')
            topic = source.get('topic')
            offset = source.get("offset")

            stream = self.getKafkaStream(ssc, brokers, topic, offset)
            dStream.update({dataSource: stream})

        mockStream = ssc.textFileStream("./dataSource")
        dStream.update({"DATA_FEATURE": mockStream})

        return ssc, dStream

    def createMockDataProcess(self):
        ssc, dStream = getStreamingContextAndStream(self.conf)
        mockStream = ssc.textFileStream("./dataSource")
        dStream.update({"DATA_FEATURE": mockStream})

        self.mockDataProcess(dStream)

        return ssc

    def mockDataProcess(self, dStream):
        """
            数据模拟生成实时数据处理主流程
        """
        dataFeatureStream = dStream.get("DATA_FEATURE")
        timeLineStream = dStream.get("TIME_LINE")
        dataFeature = self.convertDataFeature(dataFeatureStream)
        # todo for debug
        dataFeature.pprint(3)
        dataFeature.count().map(lambda line: "The count of mockdata is %s" % line).pprint()

        timeLine = self.convertTime(timeLineStream)
        # todo for debug
        timeLine.pprint(3)
        timeLine.count().map(lambda line: "The count of timeLine is %s" % line).pprint()
        self.intersectionStream(dataFeature, timeLine)

    def convertDataFeature(self, dataFeatureStream):
        """
            对数据特征进行处理,使用状态机对数据特征进行缓存,缓存后的数据与时间轴进行关联
            生成模拟数据
        """

        def updateFunction(newValues, oldValue):
            if oldValue is None:
                oldValue = newValues[0]
            return oldValue

        def convertTuple(tuple):
            key = tuple[0]
            value = tuple[1]

            key = int(key.split(":")[1])

            return key, value

        dataFeature = dataFeatureStream.map(loadJson). \
            flatMap(generateWaybillInfoList). \
            map(convertDataInfoTotuple). \
            updateStateByKey(updateFunction). \
            map(convertTuple)

        dataFeature.checkpoint(self.conf.get("BATCH_INTERVAL_SECONDS") * 5)

        return dataFeature

    def convertTime(self, timeLineStream):
        """
            对时间轴数据进行预处理,时间轴间隔太短,为了更好的控制数据生成频率,使用状态机
            控制数据生成的批次间隔(比如:按照时间轴纬度得30分钟生成一批数据)
        """

        def updateFunction(newValues, oldValue):
            if oldValue is None:
                oldValue = 0
            else:
                oldValue = sum(newValues) + oldValue

            return oldValue

        timeLine = timeLineStream.filter(isTimeLine). \
            map(convertTimeLineToBatch). \
            updateStateByKey(updateFunction). \
            filter(isEffectiveTimeLine). \
            map(convertTimeLine)

        timeLine.checkpoint(self.conf.get("BATCH_INTERVAL_SECONDS") * 5)

        return timeLine

    @staticmethod
    def intersectionStream(dataFeature, timeLine):
        """
            时间轴数据与数据特征数据进行关联,对数据特征进行转换,最后将数据发送到kafka对应的主题
        """

        def sendWaybillinfo(waybillinfo):
            waybillinfo.repartition(1).\
                foreachPartition(sendMessageByKafka)

        res = timeLine.join(dataFeature).\
            map(convertRelatedInfo). \
            map(subdivisionTask). \
            map(aggregationFetureList). \
            flatMap(convertAggregationData). \
            map(generateTime). \
            map(generateZoneCode). \
            map(generateWayBillNo)
        res.pprint(3)
        res.count().map(lambda line: "The count of res is %s" % line).pprint()
        res.foreachRDD(sendWaybillinfo)

    def main(self):
        ssc = self.createMockDataProcess()
        ssc.start()
        ssc.awaitTermination()




if __name__ == '__main__':
    conf = MOCK_DATA
    MockData(conf).main()
