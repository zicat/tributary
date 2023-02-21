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

package org.zicat.tributary.channel.kafka;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.zicat.tributary.channel.ChannelConfigOption.OPTION_PARTITION_COUNT;

/** KafkaChannelFactory. */
public class KafkaChannelFactory implements ChannelFactory {

    public static final String TYPE = "kafka";
    public static final String KEY_PREFIX = TYPE + ".";

    public static final ConfigOption<String> OPTIONS_KAFKA_TOPIC_META_DIR =
            ConfigOptions.key("topicDir")
                    .stringType()
                    .description(
                            "each tributary service use diff kafka topic, this param is store kafka topic name for restart usage")
                    .noDefaultValue();

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, ReadableConfig config) throws Exception {
        final String topicDir = config.get(OPTIONS_KAFKA_TOPIC_META_DIR);
        final String kafkaTopic = getTopic(new File(topicDir, topic));
        final int partitionCounts = config.get(OPTION_PARTITION_COUNT);
        final Set<String> groupSet = groupSet(config);
        final Properties properties = config.filterAndRemovePrefixKey(KEY_PREFIX).toProperties();
        return new KafkaChannel(kafkaTopic, partitionCounts, groupSet, properties);
    }

    /**
     * create topic with prefix.
     *
     * @param topicMetaFile topicMetaFile
     * @return string topic
     * @throws IOException IOException
     */
    public static String getTopic(File topicMetaFile) throws IOException {
        if (topicMetaFile.exists() && topicMetaFile.length() > 0) {
            return new String(IOUtils.readFull(topicMetaFile), StandardCharsets.UTF_8);
        }
        if (!topicMetaFile.exists()) {
            IOUtils.makeDir(topicMetaFile.getParentFile());
            if (!topicMetaFile.createNewFile()) {
                throw new IOException("create file error name = " + topicMetaFile.getPath());
            }
        }
        if (!topicMetaFile.isFile()) {
            throw new IOException("topic meta file is not a file name = " + topicMetaFile);
        }
        final String topic =
                topicMetaFile.getName() + "_" + UUID.randomUUID().toString().replace("-", "_");
        try (FileOutputStream fis = new FileOutputStream(topicMetaFile)) {
            fis.write(topic.getBytes(StandardCharsets.UTF_8));
        }
        return topic;
    }
}
