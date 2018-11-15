# -*- coding:utf-8 -*-

conf = {
    "APP_NAME": "Airbuildup",
    "BATCH_INTERVAL_SECONDS": 200,
    "CHECKPOINT_INTERVAL": 1000,
    "WINDOW_SIZE": 7200,
    "EXECUTION_TIME": -1,
    "DEV_MODE": 0,
    "GROUP_ID": "airbuildup",
    "OFFSET_MODE": "largest",
    "MAX_RATE_PER_PARTITION": 100000,
    "TOTAL_CORES": 60,
    "DATASOURCE_FILENAME": "/root/fengchi/transformer/conf/jobs/datasource.json",
    "CHECKPOINT_PATH": "/root/fengchi/airbuildup/checkpoint",
    "OUTPUT_DATA_PATH": "/root/fengchi/transformer/data",
    "ZONE_HUB_DIR": "/root/fengchi/airbuildup/data/ZoneHub",
    "OUTPUT_REQUEST_URL": "",
    "CONVEYANCE_EXPIRE_DAYS": 1,
    "PRESENT_CITY_CODE": "755",
    "PRESENT_PROVINCE_CITY_CODE": "020,660,662,663,668,750,751,752,753,754,755,756,757,758,759,760,762,763,766,768,769",
    "AIR_TRANSFORM_TYPE": "T4,T801",
    "PACKAGE_EXPIRE_DAYS": 1,
    "EXPERIENCE_PICKUP_TO_ARRIVAL_DELAY": 36000,
    "EXPERIENCE_PACKAGING_TO_ARRIVAL_DELAY": 18000,
    "EXPERIENCE_LOADGOODS_TO_ARRIVAL_DELAY": 1800,
    "EXPERIENCE_LOCKING_TO_ARRIVAL_DELAY": 4800,
    "EXPERIENCE_UNLOCKING_TO_ARRIVAL_DELAY": 1800,

    "DATA_SOURCE_LIST": ["waybill", "bargun", "conveyance", "inc_ubas"],
    "waybill": {
        "broker": "10.116.140.220:9095,10.116.140.221:9095,10.116.140.222:9095,10.116.140.223:9095,10.116.140.224:9095,"
                  "10.116.140.225:9095,10.116.140.226:9095,10.116.140.227:9095,10.116.140.228:9095,10.116.140.229:9095,"
                  "10.116.140.230:9095,10.116.140.231:9095,10.116.140.232:9095,10.116.140.233:9095,10.116.140.234:9095,"
                  "10.116.140.235:9095",
        "table": "tt_waybill_info",
        "topic": "SHIVA_OMS_CORE_OPERATION_WAYBILL"
    },
    "bargun": {
        "broker": "10.116.65.200:9094,10.116.65.201:9094,10.116.65.202:9094,10.116.65.203:9094,10.116.65.204:9094,"
                  "10.116.65.205:9094,10.116.65.206:9094,10.116.65.207:9094,10.116.65.208:9094,10.116.65.209:9094,"
                  "10.116.65.210:9094,10.116.65.211:9094,10.116.65.212:9094,10.116.65.213:9094,10.116.65.214:9094,"
                  "10.116.65.215:9094,10.116.65.216:9094,10.116.65.217:9094,10.116.65.218:9094,10.116.65.219:9094,"
                  "10.116.65.220:9094,10.116.65.221:9094,10.116.65.222:9094,10.116.65.223:9094,10.116.65.224:9094,"
                  "10.116.65.225:9094,10.116.65.226:9094,10.116.65.227:9094",
        "table": "fvp_core_fact_route",
        "topic": "FVP_CORE_EXPRESS_FACT_ROUTE"
    },
    "conveyance": {
        "broker": "10.116.140.220:9095,10.116.140.221:9095,10.116.140.222:9095,10.116.140.223:9095,10.116.140.224:9095,"
                  "10.116.140.225:9095,10.116.140.226:9095,10.116.140.227:9095,10.116.140.228:9095,10.116.140.229:9095,"
                  "10.116.140.230:9095,10.116.140.231:9095,10.116.140.232:9095,10.116.140.233:9095,10.116.140.234:9095,"
                  "10.116.140.235:9095",
        "table": "cc",
        "topic": "SHIVA_OMCS_RUSSIAN_VEHICLE_TASK",
        "topic-production": "EOS_TDOP_FAKE_CONVEYANCE_BATCH"
    },
    "line_cargo": {
        "broker": "10.116.140.220:9095,10.116.140.221:9095,10.116.140.222:9095,10.116.140.223:9095,10.116.140.224:9095,"
                  "10.116.140.225:9095,10.116.140.226:9095,10.116.140.227:9095,10.116.140.228:9095,10.116.140.229:9095,"
                  "10.116.140.230:9095,10.116.140.231:9095,10.116.140.232:9095,10.116.140.233:9095,10.116.140.234:9095,"
                  "10.116.140.235:9095",
        "table": "tt_waybill_route_info",
        "topic": "EOS_TDOP_FAKE_LINE_CARGO",
        "topic-production": "EOS_TDOP_FAKE_LINE_CARGO"
    },
    "inc_ubas": {
        "broker": "10.116.77.17:9092,10.116.77.18:9092,10.116.77.19:9092,10.116.77.20:9092,10.116.77.21:9092",
        "table": "inc_ubas_bulktransit",
        "topic": "inc_ubas_bulktransit",
        "topic-production": "inc_ubas_bulktransit"
    }
}
