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

package org.zicat.tributary.service.configuration;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.service.source.DefaultTributaryServer;
import org.zicat.tributary.service.source.TributaryServer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** SourceConfiguration. */
@ConfigurationProperties
@Configuration
@Data
public class DynamicSource {

    private static final String KEY_NETTY_PORT = "netty.port";

    private static final String DEFAULT_NETTY_IDLE_SECOND = "120";
    private static final String KEY_NETTY_IDLE_SECOND = "netty.idle.second";

    private static final String DEFAULT_NETTY_THREADS = "10";
    private static final String KEY_NETTY_THREADS = "netty.threads";

    private static final String DEFAULT_NETTY_HOST = "";
    private static final String KEY_NETTY_HOST = "netty.host";

    private static final String KEY_CHANNEL = "channel";

    @Autowired DynamicChannel dynamicChannel;

    Map<String, String> source;
    Map<String, TributaryServer> serverMap = new HashMap<>();

    @PostConstruct
    public void init() throws InterruptedException {
        final Set<String> sourceSet = getSources();
        for (String sourceId : sourceSet) {
            final String host = dynamicSourceValue(sourceId, KEY_NETTY_HOST, DEFAULT_NETTY_HOST);
            final int port = Integer.parseInt(dynamicSourceValue(sourceId, KEY_NETTY_PORT));
            final int threads =
                    Integer.parseInt(
                            dynamicSourceValue(sourceId, KEY_NETTY_THREADS, DEFAULT_NETTY_THREADS));
            final String channel = dynamicSourceValue(sourceId, KEY_CHANNEL);
            final int idleSecond =
                    Integer.parseInt(
                            dynamicSourceValue(
                                    sourceId, KEY_NETTY_IDLE_SECOND, DEFAULT_NETTY_IDLE_SECOND));
            final TributaryServer server =
                    new DefaultTributaryServer(
                            host, port, threads, dynamicChannel.getChannel(channel), idleSecond);
            server.start();
            serverMap.put(sourceId, server);
        }
    }

    @PreDestroy
    public void destroy() {
        for (Map.Entry<String, TributaryServer> entry : serverMap.entrySet()) {
            IOUtils.closeQuietly(entry.getValue());
        }
    }

    /**
     * source set.
     *
     * @return source set
     */
    private Set<String> getSources() {
        final Set<String> sourceSet = new HashSet<>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
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
        final String realKey = String.join(".", sourceId, key);
        final String value = source.get(realKey);
        if (value == null && defaultValue == null) {
            throw new RuntimeException("key not configuration, key = " + realKey);
        }
        return value == null ? defaultValue : value;
    }

    /**
     * create dynamic source value.
     *
     * @param sourceId sourceId
     * @param key key
     * @return value
     */
    private String dynamicSourceValue(String sourceId, String key) {
        return dynamicSourceValue(sourceId, key, null);
    }
}
