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

package org.zicat.tributary.sink.hdfs.test;

import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.sink.config.Context;
import org.zicat.tributary.sink.hdfs.HDFSRecordsWriter;
import org.zicat.tributary.sink.hdfs.HDFSRecordsWriterFactory;

import static org.zicat.tributary.sink.hdfs.ParquetHDFSRecordsWriterFactory.OPTION_OUTPUT_COMPRESSION_CODEC;

/** MockHDFSWriterFactory. */
public class MockHDFSRecordsWriterFactory implements HDFSRecordsWriterFactory {

    public static final ConfigOption<Object> OPTION_WRITER =
            ConfigOptions.key("writer.instance").objectType().noDefaultValue();
    public static final String ID = "mock_hdfs_writer";

    @Override
    public String identity() {
        return ID;
    }

    @Override
    public HDFSRecordsWriter create(Context context) {
        return (HDFSRecordsWriter) context.get(OPTION_WRITER);
    }

    @Override
    public String fileExtension(Context context) {
        final String codec = context.get(OPTION_OUTPUT_COMPRESSION_CODEC);
        return codec == null ? ".mock" : "." + codec + ".mock";
    }
}
