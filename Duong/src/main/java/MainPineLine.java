import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                        .apply("JsonToCommonLog", ParDo.of(new DoFn<String, com.mypackage.pipeline.Account>() {
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
}
