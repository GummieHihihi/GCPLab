package Emulator;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class MainPineLineEmulator {
    static final TupleTag<Account> parsedMessages = new TupleTag<Account>() {
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

        @Description("BigQuery table name")
        String getOutputTableName();

        void setOutputTableName(String outputTableName);

        @Description("Input topic name")
        String getInputTopic();

        void setInputTopic(String inputTopic);

//        @Description("The Cloud Storage bucket used for writing " + "unparseable Pubsub Messages.")
//        String getDeadletterBucket();

//        void setDeadletterQueue(String deadLetterQueue);

    }

        /**
         * A PTransform accepting Json and outputting tagged CommonLog with Beam Schema or raw Json string if parsing fails
         */
        public static class PubsubMessageToAccount extends PTransform<PCollection<String>, PCollectionTuple> {
            @Override
            public PCollectionTuple expand(PCollection<String> input) {
                return input
                        .apply("Json to object account", ParDo.of(new DoFn<String, Account>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        String json = context.element();
                                        Gson gson = new Gson();
                                        try {
                                            Account account = gson.fromJson(json, Account.class);
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
//        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
        options.setStreaming(true);
        options.setRunner(DirectRunner.class);
        options.setPubsubRootUrl("http://127.0.0.1:8085");
        run(options);
    }

    public static PipelineResult.State run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("Analyze human information" + System.currentTimeMillis());

        PCollectionTuple transformOut =
                pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                // Retrieve timestamp information from Pubsub Message attribute
                                .fromSubscription("projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-1"))
//                        .apply("Print", ParDo.of(new DoFn<String, String>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                String line = c.element();
//                System.out.println(line);
//            }
//        }))
                        .apply("ConvertMessageToAccount", new PubsubMessageToAccount());

        transformOut.get(parsedMessages)
                .apply("Print", ParDo.of(new DoFn<Account, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Account account = c.element();
//                System.out.println(line);
                if (account!= null){
                    System.out.println(account.toString());
                }
            }
        }));

        transformOut.get(unparsedMessages)
                .apply("false message", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String text = c.element();
                        if (text!=null){
                            System.out.println(text);
                        }

                    }
                }));
        return pipeline.run().waitUntilFinish();
    }

}
