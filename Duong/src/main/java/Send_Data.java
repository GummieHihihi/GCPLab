import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Send_Data {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
//        String projectId = "nttdata-c4e-bde";
//        String topicId = "uc1-input-topic-1";
        final String projectId = args[0];
        final String topicId = args[1];
        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;

        try {
            publisher = Publisher.newBuilder(topicName).build();
        } catch (IOException e) {
            e.printStackTrace();
        }


        GenerateData dataFactory = new GenerateData();

        for (int i = 0; i < 50; i++) {
            System.out.printf("Publish message %dth in Topic \n", i);
//            #generate right wrong topic
            Random random = new Random();
            boolean rightWrongCase = random.nextBoolean();
            String record = "";
            if (rightWrongCase) {
                record = dataFactory.createRightMessage(i);
                System.out.println(record);

            } else {
                record = dataFactory.createWrongMessage();
                System.out.println(record);
            }
            ByteString data = ByteString.copyFromUtf8(record);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            ApiFuture<String> future = publisher.publish(pubsubMessage);
            String messageId = future.get();
        }
        if (publisher != null) {
            // When finished with the publisher, shutdown to free up resources.
            publisher.shutdown();
            publisher.awaitTermination(1, TimeUnit.MINUTES);
        }
    }
}
