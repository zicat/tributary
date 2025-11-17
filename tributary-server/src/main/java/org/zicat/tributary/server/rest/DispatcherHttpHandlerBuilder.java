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

package org.zicat.tributary.server.rest;

import io.prometheus.client.CollectorRegistry;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.server.rest.handler.MetricRestHandler;
import org.zicat.tributary.server.rest.handler.RestHandler;

import java.util.HashMap;
import java.util.Map;

/** DispatcherHttpHandlerBuilder. */
public class DispatcherHttpHandlerBuilder {

    public static final ConfigOption<String> OPTION_METRICS_PATH =
            ConfigOptions.key("path.metrics").stringType().defaultValue("/metrics");

    private CollectorRegistry metricCollectorRegistry = CollectorRegistry.defaultRegistry;

    private final ReadableConfig serverConfig;

    public DispatcherHttpHandlerBuilder(ReadableConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    /**
     * set metric collector registry.
     *
     * @param metricCollectorRegistry metricCollectorRegistry
     * @return this
     */
    public DispatcherHttpHandlerBuilder metricCollectorRegistry(
            CollectorRegistry metricCollectorRegistry) {
        if (metricCollectorRegistry == null) {
            return this;
        }
        this.metricCollectorRegistry = metricCollectorRegistry;
        return this;
    }

    /**
     * build DispatcherHttpHandler.
     *
     * @return DispatcherHttpHandler
     */
    public DispatcherHttpHandler build() {
        final Map<String, RestHandler> mapping = new HashMap<>();
        mapping.put(
                serverConfig.get(OPTION_METRICS_PATH),
                new MetricRestHandler(metricCollectorRegistry));
        return new DispatcherHttpHandler(mapping);
    }
}
