# -*- coding:utf-8 -*-


#"115.159.89.221:9092,115.159.89.221:9093,115.159.89.221:9094"
# 115.159.30.192:9092
config = {

    "APP_NAME": "mockdata",
    "MAX_RATE_PER_PARTITION": 1000,
    "TOTAL_CORES": 2,
    "BATCH_INTERVAL_SECONDS": 3,
    "CHECK_POINT_PATH": "./checkpoint",
    "GROUP_ID": "simulation.mockdata",
    "KAFKA_BROKERS": "115.159.89.221:9092,115.159.89.221:9093,115.159.89.221:9094",
    "WAYBILL_TOPIC": "test.simId.site.main",
    "DATA_SOURCE_LIST": ["TIME_LINE"],
    "DATA_FEATURE": {
        "broker": "115.159.89.221:9092,115.159.89.221:9093,115.159.89.221:9094",
        "topic": "dataFeature",
        "offset": "largest"
    },
    "TIME_LINE": {
        "broker": "115.159.89.221:9092,115.159.89.221:9093,115.159.89.221:9094",
        "topic": "test.simId.site.meta",
        "offset": "largest"
    }

}
