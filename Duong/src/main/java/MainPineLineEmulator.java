import com.google.api.core.ApiFuture;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class MainPineLineEmulator {
    static final TupleTag<TableRow> parsedMessages = new TupleTag<TableRow>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };


    /*
     * The logger to output status messages to.
     */

    public interface Options extends PipelineOptions, PubsubOptions, DataflowPipelineOptions {
        @Description("BigQuery project")
        String getBQProject();

        void setBQProject(String value);

        @Description("BigQuery dataset")
        String getBQDataset();

        void setBQDataset(String value);

        @Description("Pubsub project")
        String getPubSubProject();

        void setPubSubProject(String value);

        @Description("Pubsub subscription")
        String getSubscription();

        void setSubscription(String value);

        @Description("DLQ name")
        String getDLQ();

        void setDLQ(String value);
    }

        /**
         * A PTransform accepting Json and outputting tagged CommonLog with Beam Schema or raw Json string if parsing fails
         */
        public static class PubsubMessageToAccount extends PTransform<PCollection<String>, PCollectionTuple> {
            @Override
            public PCollectionTuple expand(PCollection<String> input) {
                return input
                        .apply("Json to object account", ParDo.of(new DoFn<String, TableRow>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        String jsonString = context.element();
                                        Gson gson = new Gson();
                                        try {
                                            JSONObject account = new JSONObject(jsonString);
                                            TableRow row = new TableRow()
                                                    .set("id", account.get("userId"))
                                                    .set("name", account.getString("fullName"))
                                                    .set("surname", account.getString("surName"));
                                            System.out.println(row.get("id"));
                                            context.output(parsedMessages, row);
                                        } catch (Exception e) {
                                            context.output(unparsedMessages, jsonString);
                                        }

                                    }
                                })
                                .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));
            }
        }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
        System.out.println(options.getBQProject());

        final String SUBSCRIPTION = String.format("projects/%s/subscriptions/%s", options.getPubSubProject(), options.getSubscription());

        final String ERROR_QUEUE = String.format("projects/%s/topics/%s", options.getPubSubProject(), options.getDLQ());
        final String BQ_PROJECT = options.getBQProject();
        final String BQ_DATASET = options.getBQDataset();

        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("Analyze human information" + System.currentTimeMillis());

        // read from pub/sub
        PCollectionTuple transformOut =
                pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                // Retrieve timestamp information from Pubsub Message attribute
                                .fromSubscription(SUBSCRIPTION))
                        .apply("ConvertMessageToAccount", new PubsubMessageToAccount());

        transformOut.get(parsedMessages)
                .apply("WriteSuccessfulRecordsToBQ", BigQueryIO.writeTableRows()
                        .to((row) -> {
                            String tableName = "account";
                            return new TableDestination(String.format("%s:%s.%s", BQ_PROJECT , BQ_DATASET, tableName), "Some destination");
                        })
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()) //Retry all failures except for known persistent errors.
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                );

        transformOut.get(unparsedMessages)
                .apply("Write error to PubSub", PubsubIO.writeStrings().to(ERROR_QUEUE));
        pipeline.run();
    }

}
