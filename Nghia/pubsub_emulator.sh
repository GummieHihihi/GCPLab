#!/usr/bin/zsh
echo "creat pubsub emulator with docker"



docker run -it --rm -p "8085:8085" google/cloud-sdk gcloud beta emulators pubsub start --host-port=0.0.0.0:8085

