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

package com.amazonaws.services.iot.client.core;

import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.mqtt.AwsIotMqttConnection;

/**
 * This is a thin layer on top of {@link AwsIotMqttConnection} that provides a
 * TLS v1.2 based communication channel to the MQTT implementation.
 */
public class AwsIotTcpConnection extends AwsIotMqttConnection {

    public AwsIotTcpConnection(AbstractAwsIotClient client)
            throws AWSIotException {
        super(client, "tcp://" + client.getClientEndpoint() + ":" + 1883);
    }
}
