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

package org.zicat.tributary.source.http;

import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.source.base.netty.DefaultNettySource;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;

/** HttpPipelineInitializationFactory. */
public class HttpPipelineInitializationFactory implements PipelineInitializationFactory {

    public static final ConfigOption<String> OPTIONS_PATH =
            ConfigOptions.key("netty.decoder.http.path")
                    .stringType()
                    .description(
                            "set netty http path, if not match return http 404 code, default null match all")
                    .defaultValue(null);

    public static final ConfigOption<Integer> OPTION_MAX_CONTENT_LENGTH =
            ConfigOptions.key("netty.decoder.http.content-length-max")
                    .integerType()
                    .description("set http content max size, default 16mb")
                    .defaultValue(1024 * 1024 * 16);

    public static final ConfigOption<Integer> OPTION_HTTP_WORKER_THREADS =
            ConfigOptions.key("netty.decoder.http.worker-threads")
                    .integerType()
                    .description("set http worker threads")
                    .defaultValue(10);

    public static final ConfigOption<String> OPTION_HTTP_USER =
            ConfigOptions.key("netty.decoder.http.user")
                    .stringType()
                    .description("set http basic auth user, default null not enable basic auth")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_HTTP_PASSWORD =
            ConfigOptions.key("netty.decoder.http.password")
                    .stringType()
                    .description("set http basic auth password, default null not enable basic auth")
                    .defaultValue(null);

    public static final String IDENTITY = "httpDecoder";

    @Override
    public String identity() {
        return IDENTITY;
    }

    @Override
    public PipelineInitialization createPipelineInitialization(DefaultNettySource source) {
        return new HttpPipelineInitialization(source);
    }
}
