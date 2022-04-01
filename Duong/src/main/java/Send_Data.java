import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class Send_Data {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        String projectId = "nttdata-c4e-bde";
        String topicId = "uc1-input-topic-1";
        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;

        try {
            publisher = Publisher.newBuilder(topicName).build();
        } catch (IOException e) {
            e.printStackTrace();
        }


        GenerateData dataFactory = new GenerateData();

        for (int i = 0; i < 50;i++){
            System.out.printf("Publish message %dth in Topic \n", i);
//            #generate right wrong topic
            Random random = new Random();
            boolean rightWrongCase = random.nextBoolean();
            String record = "";
            if(rightWrongCase){
                record = dataFactory.createRightMessage(i);
                System.out.println(record);

            }else{
                record = dataFactory.createWrongMessage();
                System.out.println(record);
            }
            ByteString data = ByteString.copyFromUtf8(record);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            ApiFuture<String> future = publisher.publish(pubsubMessage);
            String messageId = future.get();
        }
    }
//
//    public static void createTopicExample(String projectId, String topicId) throws IOException {
//        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
//            TopicName topicName = TopicName.of(projectId, topicId);
//            Topic topic = topicAdminClient.createTopic(topicName);
//            System.out.println("Created topic: " + topic.getName());
//        }
//    }
}
