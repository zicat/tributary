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

package org.zicat.tributary.sink.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.zicat.tributary.sink.kafka.AbstractKafkaFunction.KAFKA_KEY_BOOTSTRAP_SERVERS;

/** KafkaUtils. */
public class KafkaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

    /**
     * create kafka properties.
     *
     * @param broker broker
     * @param properties properties
     * @return Properties
     */
    public static Properties createKafkaProperties(String broker, Properties properties) {
        final Properties newProperties = new Properties();
        final Set<Map.Entry<Object, Object>> entrySet = properties.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            newProperties.put(entry.getKey(), entry.getValue());
        }
        LOG.info("start to create kafka producer, broker {}", broker);
        newProperties.setProperty(KAFKA_KEY_BOOTSTRAP_SERVERS, broker);
        return newProperties;
    }

    /**
     * create kafka properties.
     *
     * @param broker broker
     * @return Properties
     */
    public static Properties createKafkaProperties(String broker) {
        return createKafkaProperties(broker, new Properties());
    }
}
