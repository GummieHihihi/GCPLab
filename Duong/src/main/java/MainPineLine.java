import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * The {@link StreamingMinuteTrafficPipeline} is a sample pipeline which can be used as a base for creating a real
 * Dataflow pipeline.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>Requirement #1
 *   <li>Requirement #2
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT_ID
 * PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/sample-pipeline
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.mypackage.pipeline.BatchUserTrafficPipeline \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --runner=${RUNNER} \
 * ADDITIONAL PARAMETERS HERE"
 * </pre>
 */

public class MainPineLine {
    static final TupleTag<com.mypackage.pipeline.Account> parsedMessages = new TupleTag<com.mypackage.pipeline.Account>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };

    /*
     * The logger to output status messages to.
     */
    private static final Logger LOG = (Logger) LoggerFactory.getLogger(MainPineLine.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions {
        @Description("Window duration length, in seconds")
        Integer getWindowDuration();

        void setWindowDuration(Integer windowDuration);

        @Description("BigQuery table name")
        String getOutputTableName();

        void setOutputTableName(String outputTableName);

        @Description("Input topic name")
        String getInputTopic();

        void setInputTopic(String inputTopic);


        @Description("Window allowed lateness, in days")
        Integer getAllowedLateness();

        void setAllowedLateness(Integer allowedLateness);

        @Description("The Cloud Storage bucket used for writing " + "unparseable Pubsub Messages.")
        String getDeadletterBucket();

        void setDeadletterBucket(String deadletterBucket);
    }

        /**
         * A PTransform accepting Json and outputting tagged CommonLog with Beam Schema or raw Json string if parsing fails
         */
        public static class PubsubMessageToAccount extends PTransform<PCollection<String>, PCollectionTuple> {
            @Override
            public PCollectionTuple expand(PCollection<String> input) {
                return input
                        .apply("Json to object account", ParDo.of(new DoFn<String, com.mypackage.pipeline.Account>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        String json = context.element();
                                        Gson gson = new Gson();
                                        try {
                                            com.mypackage.pipeline.Account account = gson.fromJson(json, com.mypackage.pipeline.Account.class);
                                            context.output(parsedMessages, account);
                                        } catch (JsonSyntaxException e) {
                                            context.output(unparsedMessages, json);
                                        }

                                    }
                                })
                                .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));
            }
        }

    public static final Schema pageviewsSchema = Schema.builder()
            .addInt64Field("pageviews")
            //TODO: change window_end in other labs
            .addDateTimeField("window_end")
            .build();
    public static final Schema rawSchema = Schema.builder()
            .addStringField("user_id")
            .addDateTimeField("event_timestamp")
            .addDateTimeField("processing_timestamp")
            .build();

    public static void main(String[] args) {
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
        System.out.println(options.getInputTopic() + options.getDeadletterBucket());
        run(options);
    }

    public static PipelineResult run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("Analyze human information" + System.currentTimeMillis());

        /*
         * Steps:
         *  1) Read something
         *  2) Transform something
         *  3) Write something
         */

        LOG.info("Building pipeline...");


        PCollectionTuple transformOut =
                pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                // Retrieve timestamp information from Pubsub Message attributes
                                .withTimestampAttribute("timestamp")
                                .fromTopic(options.getInputTopic()))
                        .apply("convert Message to Account", new PubsubMessageToAccount());

        // Write parsed messages to BigQuery
        String testing = transformOut
                // Retrieve parsed messages
                .get(parsedMessages)
                        .toString();
//                .apply("WindowByMinute", Window.<com.mypackage.pipeline.Account>into(
//                                FixedWindows.of(Duration.standardSeconds(options.getWindowDuration()))).withAllowedLateness(
//                                Duration.standardDays(options.getAllowedLateness()))
//                        .triggering(AfterWatermark.pastEndOfWindow()
//                                .withLateFirings(AfterPane.elementCountAtLeast(1)))
//                        .accumulatingFiredPanes())
//                // update to Group.globally() after resolved: https://issues.apache.org/jira/browse/BEAM-10297
//                // Only if supports Row output
//                .apply("CountPerMinute", Combine.globally(Count.<com.mypackage.pipeline.Account>combineFn())
//                        .withoutDefaults())
//                .apply("ConvertToRow", ParDo.of(new DoFn<Long, Row>() {
//                    @ProcessElement
//                    public void processElement(@Element Long views, OutputReceiver<Row> r, IntervalWindow window) {
//                        Instant i = Instant.ofEpochMilli(window.end()
//                                .getMillis());
//                        Row row = Row.withSchema(pageviewsSchema)
//                                .addValues(views, i)
//                                .build();
//                        r.output(row);
//                    }
//                }))
//                .setRowSchema(pageviewsSchema)
//                // TODO: is this a streaming insert?
//                .apply("WriteToBQ", BigQueryIO.<Row>write().to(options.getOutputTableName())
//                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        // Write unparsed messages to Cloud Storage
        transformOut
                // Retrieve unparsed messages
                .get(unparsedMessages)
                .apply("FireEvery10s", Window.<String>configure().triggering(
                                Repeatedly.forever(
                                        AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(10))))
                        .discardingFiredPanes())
                .apply("WriteDeadletterStorage", TextIO.write()
                        //TODO: change this to actual full parameter
                        .to(options.getDeadletterBucket() + "/deadletter/*")
                        .withWindowedWrites()
                        .withNumShards(10));


        return pipeline.run();
    }

}
