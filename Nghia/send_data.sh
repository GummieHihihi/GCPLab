#!/bin/bash

echo publish simulated data

# init pub/sub emulator environment
gcloud beta emulators pubsub env-init
export PUBSUB_EMULATOR_HOST="localhost:8085"
export PUBSUB_PROJECT_ID='nttdata-c4e-bde'


# clean screen
/usr/bin/clear

/home/nghiaht7/data-engineer/.venv/bin/python  send_data.py