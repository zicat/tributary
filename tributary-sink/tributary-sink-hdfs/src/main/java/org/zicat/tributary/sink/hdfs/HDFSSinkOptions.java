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

package org.zicat.tributary.sink.hdfs;

import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.MemorySize;
import org.zicat.tributary.sink.function.Clock;
import org.zicat.tributary.sink.function.SystemClock;

import java.time.Duration;

/** HDFSSinkOptions. */
public class HDFSSinkOptions {

    public static final ConfigOption<String> OPTION_SINK_PATH =
            ConfigOptions.key("sink.path")
                    .stringType()
                    .description("set sink base path")
                    .noDefaultValue();

    public static final ConfigOption<String> OPTION_KEYTAB =
            ConfigOptions.key("keytab")
                    .stringType()
                    .description("kerberos keytab")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_PRINCIPLE =
            ConfigOptions.key("principle")
                    .stringType()
                    .description("kerberos principle")
                    .defaultValue(null);

    public static final ConfigOption<MemorySize> OPTION_ROLL_SIZE =
            ConfigOptions.key("roll.size")
                    .memoryType()
                    .description("roll new file if file size over this param")
                    .defaultValue(new MemorySize(1024 * 1024 * 256L));

    public static final ConfigOption<Integer> OPTION_MAX_RETRIES =
            ConfigOptions.key("max.retries")
                    .integerType()
                    .description("max retries times if operation fail")
                    .defaultValue(3);

    public static final ConfigOption<String> OPTION_WRITER_IDENTITY =
            ConfigOptions.key("writer.identity")
                    .stringType()
                    .description("set writer identity")
                    .defaultValue("parquet");

    public static final ConfigOption<Duration> OPTION_IDLE_TRIGGER =
            ConfigOptions.key("idle.trigger")
                    .durationType()
                    .description("idle trigger, default 30s")
                    .defaultValue(Duration.ofSeconds(30));

    public static final ConfigOption<String> OPTION_BUCKET_DATE_FORMAT =
            ConfigOptions.key("bucket.date.format")
                    .stringType()
                    .description("set process time bucket format, default yyyyMMdd_HH")
                    .defaultValue("yyyyMMdd_HH");

    public static final ConfigOption<String> OPTION_BUCKET_DATE_TIMEZONE =
            ConfigOptions.key("bucket.date.timezone")
                    .stringType()
                    .description("set process time bucket timezone, default UTC")
                    .defaultValue("UTC");

    public static final ConfigOption<Clock> OPTION_CLOCK =
            ConfigOptions.key("clock")
                    .<Clock>objectType()
                    .description("set clock instance")
                    .defaultValue(new SystemClock());
}
