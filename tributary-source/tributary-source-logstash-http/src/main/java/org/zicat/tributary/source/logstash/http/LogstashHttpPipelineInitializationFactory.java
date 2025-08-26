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

package org.zicat.tributary.source.logstash.http;

import static org.zicat.tributary.common.ConfigOptions.COMMA_SPLIT_HANDLER;

import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.source.base.netty.DefaultNettySource;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;

import java.util.Collections;
import java.util.List;

/** LogstashHttpPipelineInitializationFactory. */
public class LogstashHttpPipelineInitializationFactory implements PipelineInitializationFactory {

    public static final String CONFIG_PREFIX = "netty.decoder.logstash-http.";

    public static final ConfigOption<Integer> OPTION_LOGSTASH_HTTP_WORKER_THREADS =
            ConfigOptions.key(CONFIG_PREFIX + "worker-threads")
                    .integerType()
                    .description("set http worker thread")
                    .defaultValue(10);

    public static final ConfigOption<Integer> OPTION_LOGSTASH_HTTP_MAX_CONTENT_LENGTH =
            ConfigOptions.key(CONFIG_PREFIX + "content-length-max")
                    .integerType()
                    .description("set http content max size, default 16mb")
                    .defaultValue(1024 * 1024 * 16);

    public static final ConfigOption<Codec> OPTION_LOGSTASH_CODEC =
            ConfigOptions.key(CONFIG_PREFIX + "codec")
                    .enumType(Codec.class)
                    .description("set logstash codec, default plain")
                    .defaultValue(Codec.PLAIN);

    public static final ConfigOption<String> OPTION_LOGSTASH_HTTP_REMOTE_HOST_TARGET_FIELD =
            ConfigOptions.key(CONFIG_PREFIX + "remote_host_target_field")
                    .stringType()
                    .description("set remote host target field, default null")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_LOGSTASH_HTTP_REQUEST_HEADERS_TARGET_FIELD =
            ConfigOptions.key(CONFIG_PREFIX + "request_headers_target_field")
                    .stringType()
                    .description("set request headers target field, default null")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_LOGSTASH_HTTP_USER =
            ConfigOptions.key(CONFIG_PREFIX + "user")
                    .stringType()
                    .description("set http basic auth user, default null not enable basic auth")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_LOGSTASH_HTTP_PASSWORD =
            ConfigOptions.key(CONFIG_PREFIX + "password")
                    .stringType()
                    .description("set http basic auth password, default null not enable basic auth")
                    .defaultValue(null);

    public static final ConfigOption<List<String>> OPTION_LOGSTASH_HTTP_TAGS =
            ConfigOptions.key(CONFIG_PREFIX + "tags")
                    .listType(COMMA_SPLIT_HANDLER)
                    .description("set logstash http tags, default empty, split by ,")
                    .defaultValue(Collections.emptyList());

    public static final String IDENTITY = "logstashHttpDecoder";

    @Override
    public PipelineInitialization createPipelineInitialization(DefaultNettySource source)
            throws Exception {
        return new LogstashHttpPipelineInitialization(source);
    }

    @Override
    public String identity() {
        return IDENTITY;
    }
}
