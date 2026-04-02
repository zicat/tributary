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

import org.zicat.tributary.common.Clock;
import org.zicat.tributary.common.SystemClock;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.ReadableConfig;
import static org.zicat.tributary.common.records.RecordsUtils.appendHeadKeyValue;

/** RecTsInterceptorFactory. */
public class TimestampInterceptorFactory implements SourceInterceptorFactory {

    public static final String HEADER_KEY_REC_TS = "_rec_ts";
    public static final String IDENTITY = "timestamp";
    public static final ConfigOption<Clock> OPTION_SOURCE_CLOCK =
            ConfigOptions.key("_source_clock").<Clock>objectType().defaultValue(new SystemClock());
    public static final ConfigOption<String> OPTION_TIMESTAMP_KEY =
            ConfigOptions.key("interceptor.timestamp.key")
                    .stringType()
                    .defaultValue(HEADER_KEY_REC_TS);

    @Override
    public SourceInterceptor createInterceptor(ReadableConfig config) {
        final Clock clock = config.get(OPTION_SOURCE_CLOCK);
        final String key = config.get(OPTION_TIMESTAMP_KEY);
        return records -> {
            final byte[] recTs = String.valueOf(clock.currentTimeMillis()).getBytes();
            appendHeadKeyValue(records.headers(), key, recTs);
            return records;
        };
    }

    @Override
    public String identity() {
        return IDENTITY;
    }
}
