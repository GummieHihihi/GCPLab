<h1>Welcome to the pineline instruction</h1>

please  follow these command to execute the pineline : 

First you need to pull the code from github : 


To send data :
---------------------------
mvn clean compile
mvn exec:java -Dexec.mainClass=Send_Data -Dexec.args="'nttdata-c4e-bde' 'uc1-input-topic-1'"

to execute dataflow job :
---------------------------
export LAB_ID=1
export DATASET=uc1_1
mvn exec:java -Dexec.mainClass="MainPineLineEmulator" -Dexec.args=" \
--runner=DataflowRunner \
--project=nttdata-c4e-bde \
--BQProject=nttdata-c4e-bde \
--BQDataset=${DATASET} \
--subscription=uc1-input-topic-sub-1 \
--jobName=usecase1-labid-100 \
--pubSubProject=nttdata-c4e-bde \
--region=europe-west4 \
--maxNumWorkers=1 \
--workerMachineType=n1-standard-1 \
--serviceAccount=c4e-uc1-sa-$LAB_ID@nttdata-c4e-bde.iam.gserviceaccount.com \
--subnetwork=regions/europe-west4/subnetworks/subnet-uc1-$LAB_ID \
--DLQ=uc1-dlq-topic-1 \
--gcpTempLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/temp \
--stagingLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/staging \
--streaming=true"