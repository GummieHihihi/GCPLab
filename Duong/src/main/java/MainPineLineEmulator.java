import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.json.JSONObject;

public class MainPineLineEmulator {
    static final TupleTag<TableRow> parsedMessages = new TupleTag<TableRow>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };


    /*
     * The logger to output status messages to.
     */

    public interface Options extends PipelineOptions, PubsubOptions {
        /**
         * The {@link Options} class provides the custom execution options passed by the executor at the
         * command-line.
         */
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
                                        Gson gson = new Gson();
                                        String jsonString = context.element();
                                        try {
                                            TableRow account = gson.fromJson(jsonString, TableRow.class);
                                            System.out.println(account);
                                            context.output(parsedMessages, account);
                                        } catch (JsonSyntaxException e) {
                                            System.out.println(jsonString);
                                            context.output(unparsedMessages, jsonString);
                                        }
                                        catch (IllegalArgumentException e){
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
        Pipeline p = Pipeline.create(options);
        options.setStreaming(true);
        options.setRunner(DirectRunner.class);
        run(options);
    }

    public static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("Analyze human information" + System.currentTimeMillis());

        // read from pub/sub
        PCollectionTuple transformOut =
                pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                // Retrieve timestamp information from Pubsub Message attribute
                                .fromSubscription("projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-1"))
                        .apply("ConvertMessageToAccount", new PubsubMessageToAccount());

            transformOut.get(parsedMessages)
                    .apply("WriteSuccessfulRecordsToBQ", BigQueryIO.writeTableRows()
                            .to((row) -> {
                                String tableName = "account";
                                return new TableDestination(String.format("%s:%s.%s", "nttdata-c4e-bde", "uc1_0", tableName), "Some destination");
                            })
                            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()) //Retry all failures except for known persistent errors.
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
        );


//        transformOut.get(unparsedMessages)
//                .apply("false message handling", ParDo.of(new DoFn<String, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        String text = c.element();
//                        if (text!=null){
//                            System.out.println(text);
//                        }
//
//                    }
//                }));
        return pipeline.run();
    }

}
