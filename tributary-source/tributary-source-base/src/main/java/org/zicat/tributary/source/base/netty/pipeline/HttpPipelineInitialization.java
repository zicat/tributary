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

package org.zicat.tributary.source.base.netty.pipeline;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.source.base.netty.DefaultNettySource;
import org.zicat.tributary.source.base.netty.handler.HttpMessageDecoder;
import org.zicat.tributary.source.base.netty.handler.IdleCloseHandler;

/** HttpPipelineInitialization. */
public class HttpPipelineInitialization extends AbstractPipelineInitialization {

    public static final ConfigOption<String> OPTIONS_PATH =
            ConfigOptions.key("netty.decoder.http.path")
                    .stringType()
                    .description(
                            "set netty http path, if not match return http 404 code, default null match all")
                    .defaultValue(null);
    private static final ConfigOption<Integer> OPTION_MAX_CONTENT_LENGTH =
            ConfigOptions.key("netty.decoder.http.content.length.max")
                    .integerType()
                    .description("set http content max size, default 16mb")
                    .defaultValue(1024 * 1024 * 16);

    private final String path;
    private final int maxContentLength;
    private final DefaultNettySource source;

    public HttpPipelineInitialization(DefaultNettySource source) {
        super(source);
        this.source = source;
        final ReadableConfig conf = source.getConfig();
        this.path = formatPath(conf.get(OPTIONS_PATH));
        this.maxContentLength = conf.get(OPTION_MAX_CONTENT_LENGTH);
    }

    @Override
    public void init(ChannelPipeline pip) {
        pip.addLast(new HttpResponseEncoder());
        pip.addLast(source.idleStateHandler());
        pip.addLast(new IdleCloseHandler());
        pip.addLast(new HttpRequestDecoder());
        pip.addLast(new HttpObjectAggregator(maxContentLength));
        pip.addLast(new HttpMessageDecoder(source, selectPartition(), path));
    }

    /**
     * formatPath.
     *
     * @param path path
     * @return string
     */
    public static String formatPath(String path) {
        if (path == null) {
            return null;
        }
        final String trimPath = path.trim();
        return trimPath.endsWith("/") ? trimPath.substring(1, trimPath.length() - 1) : trimPath;
    }
}
