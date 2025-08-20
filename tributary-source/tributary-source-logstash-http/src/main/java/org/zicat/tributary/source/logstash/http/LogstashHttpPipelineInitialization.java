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

import static org.zicat.tributary.common.Strings.blank2Null;
import static org.zicat.tributary.source.logstash.http.LogstashHttpPipelineInitializationFactory.*;

import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.Strings;
import org.zicat.tributary.source.base.netty.DefaultNettySource;
import org.zicat.tributary.source.http.HttpMessageDecoder;
import org.zicat.tributary.source.http.HttpPipelineInitialization;

import java.util.List;

/** LogstashHttpPipelineInitialization. */
public class LogstashHttpPipelineInitialization extends HttpPipelineInitialization {

    protected final Codec codec;
    protected final String remoteHostTargetField;
    protected final String requestHeadersTargetField;
    protected final List<String> tags;

    public LogstashHttpPipelineInitialization(DefaultNettySource source) {
        super(source);
        final ReadableConfig conf = source.getConfig();
        this.codec = conf.get(OPTION_LOGSTASH_CODEC);
        this.remoteHostTargetField =
                blank2Null(conf.get(OPTION_LOGSTASH_HTTP_REMOTE_HOST_TARGET_FIELD));
        this.requestHeadersTargetField =
                blank2Null(conf.get(OPTION_LOGSTASH_HTTP_REQUEST_HEADERS_TARGET_FIELD));
        this.tags = conf.get(OPTION_LOGSTASH_HTTP_TAGS);
    }

    @Override
    protected HttpMessageDecoder createHttpMessageDecoder() {
        return new LogstashHttpMessageDecoder(
                source,
                selectPartition(),
                path,
                authToken,
                codec,
                remoteHostTargetField,
                requestHeadersTargetField,
                tags);
    }

    @Override
    protected int httpWorkerThread(ReadableConfig config) {
        return config.get(OPTION_LOGSTASH_HTTP_WORKER_THREADS);
    }

    @Override
    protected int maxContentLength(ReadableConfig config) {
        return config.get(OPTION_LOGSTASH_HTTP_MAX_CONTENT_LENGTH);
    }

    @Override
    protected String username(ReadableConfig config) {
        return Strings.blank2Null(config.get(OPTION_LOGSTASH_HTTP_USER));
    }

    @Override
    protected String password(ReadableConfig config) {
        return Strings.blank2Null(config.get(OPTION_LOGSTASH_HTTP_PASSWORD));
    }
}
