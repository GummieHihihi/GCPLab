#!/bin/bash

echo submit dataflow job

# init pub/sub emulator environment
gcloud beta emulators pubsub env-init
export PUBSUB_EMULATOR_HOST="localhost:8085"
export PUBSUB_PROJECT_ID='nttdata-c4e-bde'


# clean screen
/usr/bin/clear


/home/nghiaht7/data-engineer/.venv/bin/python  dataflow_job.py




# export RUNNER=DirectRunner
# export RUNNING_MODE=in_memory
# export NUM_WORKERS=2
# export INPUT_SUBSCRIPTION=projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-3
# export TABLE_NAME=account
# export DEADLETTER_TOPIC="projects/nttdata-c4e-bde/topics/uc1-dlq-topic-3"


# /home/nghiaht7/data-engineer/.venv/bin/python  dataflow_job.py \
# --runner=${RUNNER} \
# --direct_running_mode=${RUNNING_MODE} \
# --direct_num_workers= ${NUM_WORKERS} \
# --input_subscription= ${INPUT_SUBSCRIPTION} \
# --table_name=${TABLE_NAME} \
# --dead_letter_topic=${DEADLETTER_TOPIC}


