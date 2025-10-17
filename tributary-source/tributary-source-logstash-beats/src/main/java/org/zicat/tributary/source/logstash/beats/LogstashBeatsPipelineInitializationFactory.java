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

package org.zicat.tributary.source.logstash.beats;

import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.source.base.netty.NettySource;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;

import java.time.Duration;

/** LogstashBeatsPipelineInitializationFactory. */
public class LogstashBeatsPipelineInitializationFactory implements PipelineInitializationFactory {

    public static final String CONFIG_PREFIX = "netty.decoder.logstash-beats.";

    public static final ConfigOption<Integer> OPTION_LOGSTASH_BEATS_WORKER_THREADS =
            ConfigOptions.key(CONFIG_PREFIX + "worker-threads")
                    .integerType()
                    .description("The number of worker threads for the Beats handler.")
                    .defaultValue(10);

    public static final ConfigOption<Boolean> OPTION_LOGSTASH_BEATS_SSL =
            ConfigOptions.key(CONFIG_PREFIX + "ssl")
                    .booleanType()
                    .description("Whether to use SSL for the Beats connection.")
                    .defaultValue(false);

    public static final ConfigOption<String> OPTION_LOGSTASH_BEATS_SSL_CERTIFICATE_AUTHORITIES =
            ConfigOptions.key(CONFIG_PREFIX + "ssl.certificate.authorities")
                    .stringType()
                    .description("The certificate authorities for the SSL connection.")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_LOGSTASH_BEATS_SSL_CERTIFICATE =
            ConfigOptions.key(CONFIG_PREFIX + "ssl.certificate")
                    .stringType()
                    .description("The certificate for the SSL connection.")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_LOGSTASH_BEATS_SSL_KEY =
            ConfigOptions.key(CONFIG_PREFIX + "ssl.key")
                    .stringType()
                    .description("The key for the SSL connection.")
                    .defaultValue(null);

    public static final ConfigOption<Duration> OPTION_LOGSTASH_BEATS_SSL_TIMEOUT =
            ConfigOptions.key(CONFIG_PREFIX + "ssl.timeout")
                    .durationType()
                    .description("The timeout for the SSL handshake, default 10s.")
                    .defaultValue(Duration.ofSeconds(10));

    public static final String IDENTITY = "logstash-beats";

    @Override
    public PipelineInitialization createPipelineInitialization(NettySource source)
            throws Exception {
        return new LogstashBeatsPipelineInitialization(source);
    }

    @Override
    public String identity() {
        return IDENTITY;
    }
}
