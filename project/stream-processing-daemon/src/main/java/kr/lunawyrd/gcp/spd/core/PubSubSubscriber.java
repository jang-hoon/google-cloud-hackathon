package kr.lunawyrd.gcp.spd.core;

import com.google.cloud.datastore.*;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PubSubSubscriber extends Thread {

    private final String projectId;
    private final String subscriptionId;

    private Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
    private KeyFactory keyFactory = datastore.newKeyFactory().setKind("Users");

    public PubSubSubscriber(String projectId, String subscriptionId) {
        this.projectId = projectId;
        this.subscriptionId = subscriptionId;
    }

    @Override
    public void run() {
        try {
            SubscriberStubSettings subscriberStubSettings  = SubscriberStubSettings.newBuilder().build();
            try(SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
                String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
                PullRequest pullRequest =
                        PullRequest.newBuilder()
                                .setMaxMessages(1000)
                                .setReturnImmediately(true)
                                .setSubscription(subscriptionName)
                                .build();

                while(true) {
                    try {
                        List<String> ackIds = new ArrayList<>();

                        PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
                        for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                            PubsubMessage pubsubMessage = message.getMessage();
                            Map<String, String> attributeMap = pubsubMessage.getAttributesMap();

                            try {
                                IncompleteKey key = keyFactory.newKey();
                                FullEntity.Builder<IncompleteKey> builder = Entity.newBuilder(key);
                                for (String attributeKey : attributeMap.keySet()) {
                                    builder.set(attributeKey, attributeMap.get(attributeKey));
                                }
                                FullEntity<IncompleteKey> incBookEntity = builder.build();
                                Entity bookEntity = datastore.add(incBookEntity);
                                System.out.println("EntityID : " + bookEntity.getKey().getId());
                                ackIds.add(message.getAckId());
                            } catch (Throwable t) {
                                t.printStackTrace();
                            }
                        }

                        if (ackIds.size() > 0) {
                            AcknowledgeRequest acknowledgeRequest =
                                    AcknowledgeRequest.newBuilder()
                                            .setSubscription(subscriptionName)
                                            .addAllAckIds(ackIds)
                                            .build();
                            subscriber.acknowledgeCallable().call(acknowledgeRequest);
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                    Thread.sleep(10);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
