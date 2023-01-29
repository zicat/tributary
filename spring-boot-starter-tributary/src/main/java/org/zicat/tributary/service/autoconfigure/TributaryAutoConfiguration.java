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

package org.zicat.tributary.service.autoconfigure;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.zicat.tributary.service.component.DynamicChannel;
import org.zicat.tributary.service.component.DynamicSinkGroupManager;
import org.zicat.tributary.service.component.DynamicSource;
import org.zicat.tributary.service.configuration.ChannelConfiguration;
import org.zicat.tributary.service.configuration.MetricsConfiguration;
import org.zicat.tributary.service.configuration.SinkGroupManagerConfiguration;
import org.zicat.tributary.service.configuration.SourceConfiguration;

/** TributaryAutoConfiguration. */
@Configuration(proxyBeanMethods = false)
@Import({
    ChannelConfiguration.class,
    MetricsConfiguration.class,
    SinkGroupManagerConfiguration.class,
    SourceConfiguration.class,
    DynamicChannel.class,
    DynamicSource.class,
    DynamicSinkGroupManager.class
})
@EnableConfigurationProperties({
    ChannelConfiguration.class,
    SinkGroupManagerConfiguration.class,
    SourceConfiguration.class,
})
public class TributaryAutoConfiguration {}
