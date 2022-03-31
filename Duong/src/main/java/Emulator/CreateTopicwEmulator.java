package Emulator;

import com.google.api.core.ApiFuture;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import java.io.IOException;
import java.util.Random;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

    public class CreateTopicwEmulator {
        public static void main(String... args) throws Exception {
            // TODO(developer): Replace these variables before running the sample.
            String projectId = "nttdata-c4e-bde";
            String topicId = "uc1-input-topic-1";
            String subcriptionId = "uc1-input-topic-1";
            Publisher publisher = null;
            TopicName topicName = TopicName.of(projectId, topicId);
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
            String hostport = System.getenv("PUBSUB_EMULATOR_HOST");
            ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            createTopicExample(projectId, topicId, hostport);
            createSubcription(projectId, topicId, hostport, subcriptionId);
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

        public static void createTopicExample(String projectId, String topicId, String hostport) throws IOException {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
            try (TopicAdminClient topicAdminClient = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build())) {
                TopicName topicName = TopicName.of(projectId, topicId);
                Topic topic = topicAdminClient.createTopic(topicName);
                System.out.println("Created topic: " + topic.getName());
            }
        }

        public static void createSubcription(String projectId, String topicId, String hostport, String subscriptionId){
            ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(
                    SubscriptionAdminSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build())) {
                TopicName topicName = TopicName.of(projectId, topicId);
                SubscriptionName subscriptionName = SubscriptionName.of(projectId, subscriptionId);
                // Create a pull subscription with default acknowledgement deadline of 10 seconds.
                // Messages not successfully acknowledged within 10 seconds will get resent by the server.
                Subscription subscription =
                        subscriptionAdminClient.createSubscription(
                                subscriptionName, topicName, PushConfig.getDefaultInstance(), 20);
                System.out.println("Created pull subscription: " + subscription.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
