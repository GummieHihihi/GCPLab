import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.ArrayList;
import java.util.List;

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
                        .apply("Json to object account", ParDo.of(new DoFn<String, Account>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        String json = context.element();
                                        Gson gson = new Gson();
                                        try {
                                            TableRow account = gson.fromJson(json, TableRow.class);
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
        Pipeline p = Pipeline.create(options);
        options.setStreaming(true);
        options.setRunner(DirectRunner.class);
        run(options);
    }

    public static PipelineResult.State run(Options options) {

        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("Analyze human information" + System.currentTimeMillis());

        // read from pub/sub
        PCollectionTuple transformOut =
                pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                // Retrieve timestamp information from Pubsub Message attribute
                                .fromSubscription("projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-1"))
//                        .apply("Print", ParDo.of(new DoFn<String, String>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                String line = c.element();
//                System.out.println(line);
//            }
//        }))
                        .apply("ConvertMessageToAccount", new PubsubMessageToAccount());

            transformOut.get(parsedMessages)
                .apply("WriteSuccessfulRecordToBQ", ParDo.of(new DoFn<TableRow, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                List<TableFieldSchema> fields = new ArrayList<>();
                fields.add(new TableFieldSchema().setName("firstName").setType("STRING"));
                fields.add(new TableFieldSchema().setName("lastName").setType("STRING"));
                fields.add(new TableFieldSchema().setName("street").setType("STRING"));
                fields.add(new TableFieldSchema().setName("fullName").setType("STRING"));
                fields.add(new TableFieldSchema().setName("userId").setType("STRING"));
                TableSchema schema = new TableSchema().setFields(fields);
                BigQueryIO.writeTableRows()
                        .to("nttdata-c4e-bde:uc1_0.testing")
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE);
            }
        }));

        transformOut.get(unparsedMessages)
                .apply("false message handling", ParDo.of(new DoFn<String, String>() {
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
