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

package org.zicat.tributary.source.base.interceptor;

import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.records.RecordsUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

/** HostInterceptorFactory. */
public class HostInterceptorFactory implements SourceInterceptorFactory {

    public static final String HEADER_KEY_HOST = "_host";
    public static final String IDENTITY = "host";
    public static final ConfigOption<String> OPTION_HOST_KEY =
            ConfigOptions.key("interceptor.host.key").stringType().defaultValue(HEADER_KEY_HOST);

    @Override
    public SourceInterceptor createInterceptor(ReadableConfig config) throws UnknownHostException {
        final String hostKey = config.get(OPTION_HOST_KEY);
        final byte[] host =
                InetAddress.getLocalHost().getHostName().getBytes(StandardCharsets.UTF_8);
        return records -> {
            RecordsUtils.appendHeadKeyValue(records.headers(), hostKey, host);
            return records;
        };
    }

    @Override
    public String identity() {
        return IDENTITY;
    }
}
