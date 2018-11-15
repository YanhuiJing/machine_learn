#!/bin/bash

############################################################
#    Set constants
############################################################
export ROOT_PATH=~
export WORKER_WORK_PATH=/usr/spark-2.[1-9].[0-9]/work
export FENGCHI_PATH="/root/fengchi"
export TRANSFORMER_PATH="${FENGCHI_PATH}/transformer"
export JARS_PATH="${TRANSFORMER_PATH}/jars"
export STREAMING_KAFKA_JAR="spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar"
export AIRBUILDUP_PATH="${TRANSFORMER_PATH}/airbuildup"

############################################################
#   Confirm requirements
############################################################
pip install -r ${TRANSFORMER_PATH}/requirements.txt

############################################################
#    Generate CMD of spark-submit
############################################################
echo -e "\n\nStart running airbuildup job..."

cd ${ROOT_PATH}
MASTER="--master spark://master:7077"
# MASTER="--master spark://master:7077,master-ha:7077"
total_cores=60
exe_cores=3
exe_memory=9
driver_memory=3
CORE_LIMIT="--total-executor-cores ${total_cores} --executor-cores ${exe_cores}"
MEMORY_LIMIT="--executor-memory ${exe_memory}G"
DRIVER_MEMORY_LIMIT="--driver-memory ${driver_memory}G"
DEPLOY_MODE="--deploy-mode client"
SUPERVISE="--supervise"
CONF="--conf spark.driver.extraJavaOptions=-Duser.timezone=GMT+8:00"
CONF_JARS="--jars ${JARS_PATH}/${STREAMING_KAFKA_JAR}"
HADOOP_CONF_DIR="/etc/hadoop/conf"

CONF_PY_FILES="--py-files ${AIRBUILDUP_PATH}/log.py"
CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/utils.py"
CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/airBargun.py"
CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/airwaybill.py"
CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/unionBeforeAirBoardStream.py"
CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/unionAirBoardingStream.py"
CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/unionAfterAirBoardStream.py"
CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/airline.py"
CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/config.py"



CONF_PY_FILES="${CONF_PY_FILES},${AIRBUILDUP_PATH}/fact_route_v2_pb2.py"
CONF_FILES="--files ${AIRBUILDUP_PATH}/fact_route_v2.proto"
MAIN_FILE="${AIRBUILDUP_PATH}/airbuildup.py"

CMD="spark-submit ${MASTER} ${CORE_LIMIT} ${MEMORY_LIMIT} ${DRIVER_MEMORY_LIMIT} ${DEPLOY_MODE} ${SUPERVISE} ${CONF} ${CONF_JARS} ${CONF_PY_FILES} ${CONF_FILES} ${MAIN_FILE}"

echo -e "\n\nGoing to submit job by executing below command in master:"
$CMD


interval=10

while true
do
    echo -e "\n\nChecking if spark job has launched..."
    jobname='airbuildup'
    PIDS=`ps -ef |grep --ignore-case ${jobname} | grep 'spark' | grep -v grep | awk '{print $2}'`
    if [ "${PIDS}" != "" ]; then
        echo "${jobname} is runing, PID is ${PIDS}"
    else
        echo "${jobname} is not runing, launching it..."
        $CMD
    fi

    status=$?
    if [ $status -ne 0 ]; then
        echo "Error in launching ${jobname}, status ${status}. Waiting for relaunch it..."
    fi

    sleep $interval
done