#create environment for pub sub
gcloud beta emulators pubsub env-init
set PUBSUB_EMULATOR_HOST=localhost:8085
set PUBSUB_PROJECT_ID=nttdata-c4e-bde

# to the project
cd C:\Users\HP\Desktop\GitCode\GCPLab\Duong

mvn clean install
mvn exec:java -D"exec.mainClass"="CreateTopic"
