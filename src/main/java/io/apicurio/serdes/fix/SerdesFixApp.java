/*
 * Copyright 2021 Red Hat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.apicurio.serdes.fix;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import io.apicurio.registry.utils.serde.AbstractKafkaSerDe;
import io.apicurio.registry.utils.serde.AbstractKafkaStrategyAwareSerDe;
import io.apicurio.registry.utils.serde.ProtobufKafkaSerializer;
import io.apicurio.registry.utils.serde.strategy.FindLatestIdStrategy;
import io.apicurio.registry.utils.serde.strategy.SimpleTopicIdStrategy;
import io.apicurio.registry.utils.serde.util.HeaderUtils;
import io.apicurio.registry.utils.serde.util.Utils;
import io.apicurio.tests.protobuf.ProtobufTestMessage;


/**
 * @author Fabian Martinez
 */
public class SerdesFixApp {

    static class ProtobufKafkaSerializerWithHeaders<U extends Message> extends ProtobufKafkaSerializer<U> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            super.configure(configs, isKey);
            if (Utils.isTrue(configs.get(USE_HEADERS))) {
                //noinspection unchecked
                headerUtils = new HeaderUtils((Map<String, Object>) configs, isKey);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        new SerdesFixApp().shouldSerdeTestMessage();
    }

    void shouldSerdeTestMessage() throws Exception {

        String registryUrl = "http://localhost:8080/api";
        String artifactIdStrategy = SimpleTopicIdStrategy.class.getName();
        String globalIdStrategy = FindLatestIdStrategy.class.getName();

        //configure producer
        Map<String, Object> producerParams = new HashMap<>();
        producerParams.put(AbstractKafkaSerDe.REGISTRY_URL_CONFIG_PARAM, registryUrl);
        producerParams.put(AbstractKafkaStrategyAwareSerDe.REGISTRY_ARTIFACT_ID_STRATEGY_CONFIG_PARAM, artifactIdStrategy);
        producerParams.put(AbstractKafkaStrategyAwareSerDe.REGISTRY_GLOBAL_ID_STRATEGY_CONFIG_PARAM, globalIdStrategy);
        producerParams.put(AbstractKafkaSerDe.USE_HEADERS, "true"); //global Id location

        ProtobufKafkaSerializerWithHeaders<ProtobufTestMessage> serializer = new ProtobufKafkaSerializerWithHeaders<>();
        serializer.configure(producerParams, false);

        //configure consumer
        Map<String, Object> consumerParams = new HashMap<>();
        consumerParams.put(AbstractKafkaSerDe.REGISTRY_URL_CONFIG_PARAM, registryUrl);
        consumerParams.put(AbstractKafkaSerDe.USE_HEADERS, "true"); //global Id location

        FileDescriptorProtobufKafkaDeserializer deserializer = new FileDescriptorProtobufKafkaDeserializer();
        deserializer.configure(consumerParams, false);

        ProtobufTestMessage testCmmn = ProtobufTestMessage
            .newBuilder()
            .setBi1(2)
            .setI1(2)
            .build();

        Headers headers = new RecordHeaders();
        final byte[] serializedData = serializer.serialize("testmessage", headers, testCmmn);

        final DynamicMessage deserialize = deserializer.deserialize("testmessage", headers, serializedData);

        serializer.close();
        deserializer.close();

        System.out.println(deserialize.toString());
        System.out.println("success");
        System.exit(1);

      }

}
