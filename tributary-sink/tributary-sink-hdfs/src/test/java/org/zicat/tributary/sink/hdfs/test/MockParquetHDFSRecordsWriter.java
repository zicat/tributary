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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.zicat.tributary.sink.hdfs.test;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.zicat.tributary.common.SystemClock;
import org.zicat.tributary.sink.hdfs.ParquetHDFSRecordsWriter;

import java.lang.reflect.Constructor;

/** MockParquetHDFSRecordsWriter. */
public class MockParquetHDFSRecordsWriter extends ParquetHDFSRecordsWriter {

    private final FileSystem fs;

    public MockParquetHDFSRecordsWriter(FileSystem fs, String codec) {
        super(CompressionCodecName.fromConf(codec), new SystemClock());
        this.fs = fs;
    }

    @Override
    public void open(FileSystem fileSystem, Path path) {
        final Configuration conf = new Configuration();
        try {
            Constructor<HadoopOutputFile> constructor =
                    HadoopOutputFile.class.getDeclaredConstructor(
                            FileSystem.class, Path.class, Configuration.class);
            constructor.setAccessible(true);
            HadoopOutputFile file = constructor.newInstance(fs, path, conf);
            this.writer =
                    AvroParquetWriter.<GenericRecord>builder(file)
                            .withSchema(SCHEMA)
                            .withCompressionCodec(compressionCodecName)
                            .withConf(conf)
                            .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
