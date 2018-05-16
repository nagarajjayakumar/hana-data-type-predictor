#! /bin/bash

WRITE_FORMAT=${1-0}
#USE_TEST=${2-99}
USE_TEST=999

BASE_DIR=/Users/njayakumar/Desktop/AMW/workspace/faas/hana-data-type-predictor/scripts/

source ${BASE_DIR}/run_env.sh

PROJECT_NAME=rdf
PROJECT_DIR=${BASE_DIR}/${PROJECT_NAME}

if [ ! -d "${PROJECT_DIR}" ]
then
    echo "ERROR: Project directory does not exist - '${PROJECT_DIR}'"
fi

RESULT_DIR=${PROJECT_DIR}/results
DATA_DIR=${RESULT_DIR}/${RUNID}

if [ ! -d ${DATA_DIR} ]
then
	mkdir -p ${DATA_DIR}
fi

echo "${DATA_DIR}"


SCRIPT_PATH="${ANALYTIC_PACKAGE}"
SCRIPT_CLASS="com.com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngine"


if [ "0" -le "${USE_TEST}" -a "${USE_TEST}" -lt 9 ]
then
#    INPUT_ARGS="--input de578816_tests.drm_hpt_s1_blade_ge90_115b_test_case_${USE_TEST} "
    OUTPUT_PATH="test_case_${USE_TEST}"
else
#    INPUT_ARGS=""
    OUTPUT_PATH="data"
fi

if [ "${WRITE_FORMAT}" == "1" ]
then
    INPUT_ARGS="${INPUT_ARGS}--write_mode CSV "
    OUTPUT_PATH="${OUTPUT_PATH}_csv"
elif [ "${WRITE_FORMAT}" == "2" ]
then
    INPUT_ARGS="${INPUT_ARGS}--write_mode ORC "
fi

HDFS_OUTPUT_DIR=/user/njayakumar/${PROJECT_NAME}/results/${OUTPUT_PATH}
SCRIPT_ARGS="${INPUT_ARGS}--output ${HDFS_OUTPUT_DIR}"

#spark-submit --packages com.databricks:spark-csv_2.11:1.4.0 --jars ${AWS_JARS} --driver-memory ${DRIVER_MEM_SIZE} --master yarn --deploy-mode client --conf spark.driver.maxResultSize=5g --conf spark.sql.broadcastTimeout=9000 --executor-memory ${EXECUTOR_MEM_SIZE} --num-executors ${NUM_EXECUTORS} ${SCRIPT_PATH} >${DATA_DIR}/run.out 2>${DATA_DIR}/run.err

run_spark_job "${SCRIPT_PATH}" "${SCRIPT_ARGS}" "${DATA_DIR}" "${SCRIPT_CLASS}"
