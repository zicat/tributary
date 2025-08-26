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

package org.zicat.tributary.source.kafka;

import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.source.base.netty.DefaultNettySource;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;

import java.time.Duration;

/** KafkaPipelineInitializationFactory. */
public class KafkaPipelineInitializationFactory implements PipelineInitializationFactory {

    public static final String CONFIG_PREFIX = "netty.decoder.kafka.";

    public static final ConfigOption<String> OPTION_KAFKA_CLUSTER_ID =
            ConfigOptions.key(CONFIG_PREFIX + "cluster.id").stringType().defaultValue(null);

    public static final ConfigOption<String> OPTION_ZOOKEEPER_CONNECT =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.connect").stringType().noDefaultValue();
    public static final ConfigOption<Duration> OPTION_CONNECTION_TIMEOUT =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.connection.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(15));
    public static final ConfigOption<Duration> OPTION_SESSION_TIMEOUT =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.session.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60));
    public static final ConfigOption<Integer> OPTION_RETRY_TIMES =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.retry.times")
                    .integerType()
                    .defaultValue(3);
    public static final ConfigOption<Duration> OPTION_FAIL_BASE_SLEEP_TIME =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.fail.base.sleep.time")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1));

    public static final ConfigOption<Duration> OPTION_META_CACHE_TTL =
            ConfigOptions.key(CONFIG_PREFIX + "meta.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10));

    public static final ConfigOption<Integer> OPTION_TOPIC_PARTITION_COUNT =
            ConfigOptions.key(CONFIG_PREFIX + "topic.partitions").integerType().defaultValue(60);

    public static final ConfigOption<Boolean> OPTION_KAFKA_SASL_PLAIN =
            ConfigOptions.key(CONFIG_PREFIX + "sasl.plain").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTION_SASL_USERS =
            ConfigOptions.key(CONFIG_PREFIX + "sasl.plain.usernames")
                    .stringType()
                    .defaultValue(null);

    public static final ConfigOption<Integer> OPTION_KAFKA_WORKER_THREADS =
            ConfigOptions.key(CONFIG_PREFIX + "worker-threads")
                    .integerType()
                    .description("The number of worker threads for the Kafka handler.")
                    .defaultValue(10);

    public static final String IDENTITY = "kafkaDecoder";

    @Override
    public String identity() {
        return IDENTITY;
    }

    @Override
    public PipelineInitialization createPipelineInitialization(DefaultNettySource source)
            throws Exception {
        return new KafkaPipelineInitialization(source);
    }
}
