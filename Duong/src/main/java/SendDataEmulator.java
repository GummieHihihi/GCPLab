import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.Random;

public class SendDataEmulator {
    public static void main(String[] args) {
        String projectId = "nttdata-c4e-bde";
        String topicId = "uc1-input-topic-1";
        Publisher publisher = null;
        TopicName topicName = TopicName.of(projectId, topicId);
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        String hostport = System.getenv("PUBSUB_EMULATOR_HOST");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        try {
            publisher =
                    Publisher.newBuilder(topicName)
                            .setChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build();
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenerateData dataFactory = new GenerateData();

        for (int i = 0; i < 100;i++){
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
            publisher.publish(pubsubMessage);
//                ApiFuture<String> future = publisher.publish(pubsubMessage);
//                String messageId = future.get();
        }
    }
}
