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

import static org.zicat.tributary.common.records.RecordsUtils.HEADER_KEY_UUID;
import static org.zicat.tributary.common.records.RecordsUtils.appendUUID;

/** UUIDInterceptorFactory. */
public class UUIDInterceptorFactory implements SourceInterceptorFactory {

    public static final String IDENTITY = "uuid";
    public static final ConfigOption<String> OPTION_UUID_KEY =
            ConfigOptions.key("interceptor.uuid.key").stringType().defaultValue(HEADER_KEY_UUID);

    @Override
    public SourceInterceptor createInterceptor(ReadableConfig config) {
        final String key = config.get(OPTION_UUID_KEY);
        return records -> {
            appendUUID(records.headers(), key);
            return records;
        };
    }

    @Override
    public String identity() {
        return IDENTITY;
    }
}
