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

package org.zicat.tributary.sink.hbase;

import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.FunctionFactory;

import static org.apache.hadoop.hbase.HConstants.CATALOG_FAMILY_STR;

/** HBaseFunctionFactory. */
public class HBaseFunctionFactory implements FunctionFactory {

    public static final ConfigOption<String> OPTION_HBASE_SITE_XML_PATH =
            ConfigOptions.key("hbase-site-xml.path").stringType().defaultValue(null);
    public static final ConfigOption<String> OPTION_HBASE_FAMILY =
            ConfigOptions.key("table.family.name").stringType().defaultValue(CATALOG_FAMILY_STR);
    public static final ConfigOption<String> OPTION_HBASE_COLUMN_VALUE_NAME =
            ConfigOptions.key("table.column.value.name").stringType().defaultValue("value");
    public static final ConfigOption<String> OPTION_HBASE_COLUMN_HEAD_PREFIX =
            ConfigOptions.key("table.column.head.name.prefix").stringType().defaultValue("head_");
    public static final ConfigOption<String> OPTION_HBASE_COLUMN_TOPIC_NAME =
            ConfigOptions.key("table.column.topic.name").stringType().defaultValue("topic");
    public static final ConfigOption<String> OPTION_HBASE_TABLE_NAME =
            ConfigOptions.key("table.name").stringType().noDefaultValue();

    public static final String IDENTITY = "hbase";

    @Override
    public Function create() {
        return new HBaseFunction();
    }

    @Override
    public String identity() {
        return IDENTITY;
    }
}
