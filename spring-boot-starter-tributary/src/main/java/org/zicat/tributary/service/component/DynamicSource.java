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

package org.zicat.tributary.service.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.*;
import org.zicat.tributary.service.configuration.SourceConfiguration;
import org.zicat.tributary.source.RecordsChannel;
import org.zicat.tributary.source.Source;
import org.zicat.tributary.source.SourceFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.zicat.tributary.common.ReadableConfig.DEFAULT_KEY_HANDLER;
import static org.zicat.tributary.common.SpiFactory.findFactory;

/** DynamicSource. */
@Component
public class DynamicSource implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicSource.class);
    private static final String SPLIT = ".";

    public static final ConfigOption<String> OPTION_IMPLEMENT =
            ConfigOptions.key("implement")
                    .stringType()
                    .description("source implement")
                    .defaultValue("default");

    public static final ConfigOption<String> OPTION_CHANNEL =
            ConfigOptions.key("channel").stringType().description("channel name").noDefaultValue();

    private final Map<String, Source> sourceCache = new HashMap<>();

    public DynamicSource(
            @Autowired DynamicChannel dynamicChannel,
            @Autowired SourceConfiguration sourceConfiguration)
            throws Exception {
        final ReadableConfig allSourceConfig =
                ReadableConfig.create(sourceConfiguration.getSource());
        final Set<String> sources = allSourceConfig.groupKeys(DEFAULT_KEY_HANDLER);
        for (String sourceId : sources) {
            final String head = sourceId + SPLIT;
            final String topic = allSourceConfig.get(OPTION_CHANNEL.concatHead(head));
            final Channel channel = dynamicChannel.getChannel(topic);
            if (channel == null) {
                throw new TributaryRuntimeException("channel." + topic + " not defined");
            }
            final String implementId = allSourceConfig.get(OPTION_IMPLEMENT.concatHead(head));
            final SourceFactory sourceFactory = findFactory(implementId, SourceFactory.class);
            try {
                final ReadableConfig sourceConfig = allSourceConfig.filterAndRemovePrefixKey(head);
                final Source source =
                        sourceFactory.createSource(
                                sourceId, RecordsChannel.create(channel), sourceConfig);
                source.start();
                sourceCache.put(sourceId, source);
            } catch (Throwable e) {
                IOUtils.closeQuietly(this);
                throw e;
            }
        }
        LOG.info("create source success, sources {}", sourceCache.keySet());
    }

    @Override
    public void close() {
        for (Map.Entry<String, Source> entry : sourceCache.entrySet()) {
            IOUtils.closeQuietly(entry.getValue());
        }
        sourceCache.clear();
    }
}
