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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.service.configuration.SourceConfiguration;
import org.zicat.tributary.source.Source;
import org.zicat.tributary.source.SourceFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.zicat.tributary.source.SourceFactory.findTributaryServerFactory;

/** DynamicSource. */
@Component
public class DynamicSource {

    private static final String SPLIT = ".";

    private static final String DEFAULT_IMPLEMENT = "default";
    private static final String KEY_IMPLEMENT = "implement";

    private static final String KEY_CHANNEL = "channel";

    @Autowired DynamicChannel dynamicChannel;
    @Autowired DynamicSinkGroupManager dynamicSinkGroupManager;
    @Autowired SourceConfiguration sourceConfiguration;

    final Map<String, Source> sourceCache = new HashMap<>();

    @PostConstruct
    public void init() throws Throwable {
        final Set<String> sourceSet = getSources();
        for (String sourceId : sourceSet) {
            final Channel channel =
                    dynamicChannel.getChannel(dynamicSourceValue(sourceId, KEY_CHANNEL, null));
            final String implementId =
                    dynamicSourceValue(sourceId, KEY_IMPLEMENT, DEFAULT_IMPLEMENT);
            final SourceFactory sourceFactory = findTributaryServerFactory(implementId);
            Source server = null;
            try {
                server = sourceFactory.createTributaryServer(channel, getSubKeyConfig(sourceId));
                server.listen();
                sourceCache.put(sourceId, server);
            } catch (Throwable e) {
                IOUtils.closeQuietly(server);
                throw e;
            }
        }
    }

    @PreDestroy
    public void destroy() {
        try {
            for (Map.Entry<String, Source> entry : sourceCache.entrySet()) {
                IOUtils.closeQuietly(entry.getValue());
            }
            sourceCache.clear();
        } finally {
            try {
                dynamicChannel.flushAll();
            } finally {
                IOUtils.closeQuietly(dynamicSinkGroupManager);
                IOUtils.closeQuietly(dynamicChannel);
            }
        }
    }

    /**
     * source set.
     *
     * @return source set
     */
    private Set<String> getSources() {
        final Set<String> sourceSet = new HashSet<>();
        for (Map.Entry<String, String> entry : sourceConfiguration.getSource().entrySet()) {
            final String key = entry.getKey();
            final String[] split = key.split("\\.");
            sourceSet.add(split[0]);
        }
        return sourceSet;
    }

    /**
     * create dynamic source value.
     *
     * @param sourceId sourceId
     * @param key key
     * @param defaultValue defaultValue
     * @return value
     */
    private String dynamicSourceValue(String sourceId, String key, String defaultValue) {
        final String realKey = String.join(SPLIT, sourceId, key);
        final String value = sourceConfiguration.getSource().get(realKey);
        if (value == null && defaultValue == null) {
            throw new RuntimeException("key not configuration, key = " + realKey);
        }
        return value == null ? defaultValue : value;
    }

    /**
     * get sub key config.
     *
     * @param sourceId sourceId
     * @return new map
     */
    private Map<String, String> getSubKeyConfig(String sourceId) {
        Map<String, String> result = new HashMap<>();
        final String prefix = sourceId + SPLIT;
        for (Map.Entry<String, String> entry : sourceConfiguration.getSource().entrySet()) {
            final String key = entry.getKey();
            if (key.startsWith(prefix)) {
                result.put(key.replace(prefix, ""), entry.getValue());
            }
        }
        return result;
    }
}