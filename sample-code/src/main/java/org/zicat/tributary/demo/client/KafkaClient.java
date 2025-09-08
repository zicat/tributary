/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.tributary.demo.client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.zicat.tributary.common.ResourceUtils.getResourcePath;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** KafkaClient. */
public class KafkaClient {

    private static Map<String, Object> configs() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        configs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        configs.put(
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                getResourcePath("ssl/kafka.client.truststore.jks"));
        configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "mytruststorepassword");
        // configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");

        configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        configs.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"16Ew658jjzvmxDqk\";");

        return configs;
    }

    public static void main(String[] args) throws InterruptedException {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs())) {
            String topic = "test-topic";
            for (int i = 0; i < 10000; i++) {
                String key = "key" + i;
                String value = "{\"value\":\"hello kafka" + i + "\"}";
                List<Header> headers = new ArrayList<>();
                headers.add(
                        new RecordHeader(
                                "header-" + i,
                                ("header-value-" + i).getBytes(StandardCharsets.UTF_8)));
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, null, key, value, headers);
                producer.send(record);
                producer.flush();
                System.out.println("Send message value: " + value);
                Thread.sleep(2000);
            }
        }
    }
}
