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

package org.zicat.tributary.server.config;

import org.zicat.tributary.common.config.ReadableConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** PropertiesConfigBuilder. */
public class PropertiesConfigBuilder {

    private static final String SOURCE_PREFIX = "source.";
    private static final String CHANNEL_PREFIX = "channel.";
    private static final String SINK_PREFIX = "sink.";
    private static final String SERVER_PREFIX = "server.";

    private final Properties properties;
    private String prefixFilter;

    public PropertiesConfigBuilder(Properties properties) {
        this.properties = properties;
    }

    /**
     * set prefix filter.
     *
     * @param prefixFilter prefix filter
     * @return PropertiesConfigBuilder
     */
    public PropertiesConfigBuilder prefixFilter(String prefixFilter) {
        this.prefixFilter = prefixFilter;
        return this;
    }

    /**
     * build.
     *
     * @return config
     */
    public ReadableConfig build() {
        final Map<String, Object> config = new HashMap<>();
        properties.forEach(
                (k, v) -> {
                    final String key = k.toString();
                    if (prefixFilter == null) {
                        config.put(key, v);
                        return;
                    }
                    if (key.startsWith(prefixFilter)) {
                        config.put(key.substring(prefixFilter.length()), v);
                    }
                });
        return ReadableConfig.create(config);
    }

    /**
     * get source config.
     *
     * @param properties properties
     * @return Config
     */
    public static ReadableConfig sourceConfig(Properties properties) {
        return config(properties, SOURCE_PREFIX);
    }

    /**
     * get channel config.
     *
     * @param properties properties
     * @return Config
     */
    public static ReadableConfig channelConfig(Properties properties) {
        return config(properties, CHANNEL_PREFIX);
    }

    /**
     * get sink config.
     *
     * @param properties properties
     * @return Config
     */
    public static ReadableConfig sinkConfig(Properties properties) {
        return config(properties, SINK_PREFIX);
    }

    /**
     * get server config.
     *
     * @param properties properties
     * @return Config
     */
    public static ReadableConfig serverConfig(Properties properties) {
        return config(properties, SERVER_PREFIX);
    }

    /**
     * get config.
     *
     * @param properties properties
     * @param prefix prefix
     * @return Config
     */
    private static ReadableConfig config(Properties properties, String prefix) {
        return new PropertiesConfigBuilder(properties).prefixFilter(prefix).build();
    }
}
