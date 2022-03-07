#!/bin/bash

echo init pub/sub emulator environment, active python and get output of dead letter queue

# init pub/sub emulator environment
gcloud beta emulators pubsub env-init
export PUBSUB_EMULATOR_HOST="localhost:8085"
export PUBSUB_PROJECT_ID='nttdata-c4e-bde'


# clean screen
/usr/bin/clear


/home/nghiaht7/data-engineer/.venv/bin/python  dlq_subscriber.py


