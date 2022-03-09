import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;

public class MainPineLine {
    static final TupleTag<com.mypackage.pipeline.Account> parsedMessages = new TupleTag<com.mypackage.pipeline.Account>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };

    /*
     * The logger to output status messages to.
     */

    public interface Options extends PipelineOptions, PubsubOptions, DataflowPinelineOptions {
        /**
         * The {@link Options} class provides the custom execution options passed by the executor at the
         * command-line.
         */

        @Description("BigQuery table name")
        String getOutputTableName();

        void setOutputTableName(String outputTableName);

        @Description("Input topic name")
        String getInputTopic();

        void setInputTopic(String inputTopic);

        @Description("The Cloud Storage bucket used for writing " + "unparseable Pubsub Messages.")
        String getDeadletterBucket();

        void setDeadletterQueue(String deadLetterQueue);
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

    public static void main(String[] args) {
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
        options.setStreaming(true);
        options.setRunner(Di.class);
        System.out.println(options.getInputTopic() + options.getDeadletterBucket());
        run(options);
    }

    public static PipelineResult run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("Analyze human information" + System.currentTimeMillis());

//        PCollectionTuple transformOut =
                pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                // Retrieve timestamp information from Pubsub Message attribute
                                .fromTopic(options.getInputTopic()))
                        .apply("Print", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                System.out.println(line);
            }
        }));

        return pipeline.run();
    }

}
