#create environment for pub sub
gcloud beta emulators pubsub env-init
set PUBSUB_EMULATOR_HOST=localhost:8085
set PUBSUB_PROJECT_ID=nttdata-c4e-bde
docker run -it --rm -p "8085:8085" google/cloud-sdk gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
