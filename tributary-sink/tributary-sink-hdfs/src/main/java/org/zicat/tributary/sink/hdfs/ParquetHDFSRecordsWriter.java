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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.Records;

import java.io.IOException;
import java.util.Map;

import static org.zicat.tributary.common.records.RecordsUtils.defaultSinkExtraHeaders;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;

/** ParquetHDFSRecordsWriter. */
public class ParquetHDFSRecordsWriter implements HDFSRecordsWriter {

    public static final String FIELD_TOPIC = "topic";
    public static final String FIELD_HEADERS = "headers";
    public static final String FIELD_KEY = "key";
    public static final String FIELD_VALUE = "value";

    protected static final Schema SCHEMA =
            SchemaBuilder.record("record")
                    .namespace("org.zicat.tributary")
                    .fields()
                    .name(FIELD_TOPIC)
                    .type()
                    .stringType()
                    .noDefault()
                    .name(FIELD_HEADERS)
                    .type()
                    .map()
                    .values()
                    .bytesType()
                    .noDefault()
                    .name(FIELD_KEY)
                    .type()
                    .bytesType()
                    .noDefault()
                    .name(FIELD_VALUE)
                    .type()
                    .bytesType()
                    .noDefault()
                    .endRecord();

    protected transient ParquetWriter<GenericRecord> writer;

    protected final CompressionCodecName compressionCodecName;

    public ParquetHDFSRecordsWriter(CompressionCodecName compressionCodecName) {
        this.compressionCodecName = compressionCodecName;
    }

    @Override
    public void open(FileSystem fileSystem, Path path) throws IOException {
        final Configuration conf = fileSystem.getConf();
        this.writer =
                AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(path, conf))
                        .withSchema(SCHEMA)
                        .withCompressionCodec(compressionCodecName)
                        .withConf(conf)
                        .build();
    }

    @Override
    public int append(Records records) throws Exception {
        final Map<String, byte[]> extraHeaders = defaultSinkExtraHeaders();
        foreachRecord(
                records,
                (key, value, allHeaders) -> {
                    final GenericRecord record = new GenericData.Record(SCHEMA);
                    record.put(FIELD_TOPIC, records.topic());
                    record.put(FIELD_HEADERS, allHeaders);
                    record.put(FIELD_KEY, key);
                    record.put(FIELD_VALUE, value);
                    writer.write(record);
                },
                extraHeaders);
        return records.count();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(writer);
        writer = null;
    }
}
