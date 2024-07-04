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

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.sink.function.Context;

/** ParquetHDFSWriterFactory. */
public class ParquetHDFSWriterFactory implements HDFSWriterFactory {

    public static final ConfigOption<String> OPTION_OUTPUT_COMPRESSION_CODEC =
            ConfigOptions.key("writer.parquet.compression.codec")
                    .stringType()
                    .description("set output compression codec, default snappy")
                    .defaultValue("snappy");

    @Override
    public String identity() {
        return "parquet";
    }

    @Override
    public HDFSWriter create(Context context) {
        return new ParquetHDFSWriter(
                CompressionCodecName.fromConf(context.get(OPTION_OUTPUT_COMPRESSION_CODEC)));
    }
}
