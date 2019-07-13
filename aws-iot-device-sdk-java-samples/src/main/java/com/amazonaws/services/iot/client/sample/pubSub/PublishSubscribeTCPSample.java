/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.iot.client.sample.pubSub;

import com.amazonaws.services.iot.client.*;
import com.amazonaws.services.iot.client.sample.sampleUtil.CommandArguments;
import com.amazonaws.services.iot.client.sample.sampleUtil.SampleUtil;

/**
 * This is an example that uses {@link AWSIotMqttClient} to subscribe to a topic and
 * publish messages to it. Both blocking and non-blocking publishing are
 * demonstrated in this example.
 */
public class PublishSubscribeTCPSample {

    private static final String TestTopic = "sdk/test/java";
    private static final AWSIotQos TestTopicQos = AWSIotQos.QOS0;

    private static AWSIotMqttClient awsIotClient;

    public static void setClient(AWSIotMqttClient client) {
        awsIotClient = client;
    }

    public static class BlockingPublisher implements Runnable {
        private final AWSIotMqttClient awsIotClient;

        public BlockingPublisher(AWSIotMqttClient awsIotClient) {
            this.awsIotClient = awsIotClient;
        }

        @Override
        public void run() {
            long counter = 1;

//            while (true) {
            for(int i = 0; i < 1; i++) {
                String payload = "hello from blocking publisher - " + (counter++);
                try {
                    awsIotClient.publish(TestTopic, payload);
                } catch (AWSIotException e) {
                    System.out.println(System.currentTimeMillis() + ": publish failed for " + payload);
                }
                System.out.println(System.currentTimeMillis() + ": >>> " + payload);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println(System.currentTimeMillis() + ": BlockingPublisher was interrupted");
                    return;
                }
            }
        }
    }

    public static class NonBlockingPublisher implements Runnable {
        private final AWSIotMqttClient awsIotClient;

        public NonBlockingPublisher(AWSIotMqttClient awsIotClient) {
            this.awsIotClient = awsIotClient;
        }

        @Override
        public void run() {
            long counter = 1;

//            while (true) {
            for(int i = 0; i < 10; i++) {
                String payload = "hello from non-blocking publisher - " + (counter++);
                AWSIotMessage message = new NonBlockingPublishListener(TestTopic, TestTopicQos, payload);
                try {
                    awsIotClient.publish(message);
                } catch (AWSIotException e) {
                    System.out.println(System.currentTimeMillis() + ": publish failed for " + payload);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println(System.currentTimeMillis() + ": NonBlockingPublisher was interrupted");
                    return;
                }
            }
        }
    }

    private static void initClient(CommandArguments arguments) {
        String clientEndpoint = arguments.getNotNull("clientEndpoint", SampleUtil.getConfig("clientEndpoint"));
        String clientId = arguments.getNotNull("clientId", SampleUtil.getConfig("clientId"));

        if (awsIotClient == null) {
            awsIotClient = new AWSIotMqttClient(clientEndpoint, clientId);
        }

        if (awsIotClient == null) {
            throw new IllegalArgumentException("Failed to construct client due to missing certificate or credentials.");
        }
    }

    public static void main(String args[]) throws InterruptedException, AWSIotException, AWSIotTimeoutException {
        CommandArguments arguments = CommandArguments.parse(args);
        initClient(arguments);

        awsIotClient.connect();

        AWSIotTopic topic = new TestTopicListener(TestTopic, TestTopicQos);
        awsIotClient.subscribe(topic, true);

//        Thread blockingPublishThread = new Thread(new BlockingPublisher(awsIotClient));
        Thread nonBlockingPublishThread = new Thread(new NonBlockingPublisher(awsIotClient));

//        blockingPublishThread.start();
        nonBlockingPublishThread.start();

//        blockingPublishThread.join();
        nonBlockingPublishThread.join();
    }

}
