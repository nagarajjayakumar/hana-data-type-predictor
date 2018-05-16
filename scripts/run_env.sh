#!/usr/bin/env bash
RUNID=`date +"%s"`

ANALYTIC_PACKAGE=${BASE_DIR}/spark_analytics-1.0-SNAPSHOT.jar

SUCCESS_FILE=__SUCCESS

if [ ! -e ${ANALYTIC_PACKAGE} ]
then
    echo "Target Package does not exist: ${ANALYTIC_PACKAGE}"
    exit 1
fi
# include AWS jars for current version of HWX
source ${BASE_DIR}/aws_config.sh

function run_spark_job() {
    JOB_PATH=$1
    JOB_ARGS=$2
    LOG_DIR=$3
    JOB_CLASS=$4

    DRIVER_MEM_SIZE=${5-12g}
    EXECUTOR_MEM_SIZE=${6-19g}
    NUM_EXECUTORS=${7-17}
    EXECUTOR_CORES=${8-5}

    if [ "${JOB_PATH}" == "" ]
    then
        echo "ERROR: Job path not specified"
        exit 1
    elif [ ! -e ${JOB_PATH} ]
    then
        echo "ERROR: Job path does not exist - '${JOB_PATH}'"
        exit 1
    fi

    if [ ! -d ${LOG_DIR} ]
    then
        echo "Log directory does not exist"
        mkdir -p ${LOG_DIR}

        if [ "$?" != "0" ]
        then
            echo "ERROR: Failed to create Log directory: ${LOG_DIR}"
            exit 1
        fi
    fi

    if [ "${JOB_CLASS}" == "" ]
    then
        JCLASS=""
    else
        JCLASS="--class ${JOB_CLASS}"
    fi

    SPARK_PACKAGES="com.databricks:spark-csv_2.10:1.5.0"

    if [ "${LOG_DIR}" == "" ]
    then
        echo "spark-submit --packages ${SPARK_PACKAGES} --jars ${AWS_JARS} --driver-memory ${DRIVER_MEM_SIZE} --master yarn --deploy-mode client --conf spark.driver.maxResultSize=5g --conf spark.sql.broadcastTimeout=9000 --executor-memory ${EXECUTOR_MEM_SIZE} --num-executors ${NUM_EXECUTORS} ${JCLASS} ${JOB_PATH} ${JOB_ARGS} ${LOG_REDIR}"
        spark-submit --packages ${SPARK_PACKAGES} --jars ${AWS_JARS} --driver-memory ${DRIVER_MEM_SIZE} --master yarn --deploy-mode client --conf spark.driver.maxResultSize=5g --conf spark.sql.broadcastTimeout=9000 --executor-memory ${EXECUTOR_MEM_SIZE} --num-executors ${NUM_EXECUTORS} ${JCLASS} ${JOB_PATH} ${JOB_ARGS}
    else
        JOB_CONFIG_FILE=${LOG_DIR}/config
        echo "Job Path: ${JOB_PATH}" > ${JOB_CONFIG_FILE}
        echo "Job Args: ${JOB_ARGS}" >> ${JOB_CONFIG_FILE}
        echo "Job Class: ${JOB_CLASS}" >> ${JOB_CONFIG_FILE}

        echo "Executors: ${NUM_EXECUTORS}" >>  ${JOB_CONFIG_FILE}
        echo "Executor Memory Size: ${EXECUTOR_MEM_SIZE}" >> ${JOB_CONFIG_FILE}
        echo "Driver Memory: ${DRIVER_MEM_SIZE}" >> ${JOB_CONFIG_FILE}

        echo "Rerun command: spark-submit --packages ${SPARK_PACKAGES} --jars ${AWS_JARS} --driver-memory ${DRIVER_MEM_SIZE} --master yarn --deploy-mode client --conf spark.driver.maxResultSize=5g --conf spark.sql.broadcastTimeout=9000 --executor-cores ${EXECUTOR_CORES} --executor-memory ${EXECUTOR_MEM_SIZE} --num-executors ${NUM_EXECUTORS} ${JCLASS} ${JOB_PATH} ${JOB_ARGS} >${LOG_DIR}/run.out 2>${LOG_DIR}/run.err" >> ${JOB_CONFIG_FILE}

        spark-submit --packages ${SPARK_PACKAGES} --jars ${AWS_JARS} --driver-memory ${DRIVER_MEM_SIZE} --master yarn --deploy-mode client --conf spark.driver.maxResultSize=5g --conf spark.sql.broadcastTimeout=9000 --executor-memory ${EXECUTOR_MEM_SIZE} --num-executors ${NUM_EXECUTORS} ${JCLASS} ${JOB_PATH} ${JOB_ARGS} >${LOG_DIR}/run.out 2>${LOG_DIR}/run.err
    fi

    if [ "$?" != "0" ]
    then
        echo "ERROR: Job execution failed"
        exit 1
    else
        echo "SUCCESS: Job executed successfully"
        if [ "${LOG_DIR}" != "" ]
        then
            # if there is a log directory create a __SUCESS file to indicate that the job ran successfully
            touch ${LOG_DIR}/${SUCCESS_FILE}
        fi
    fi
}
