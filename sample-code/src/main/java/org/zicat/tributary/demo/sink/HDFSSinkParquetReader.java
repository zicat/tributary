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

package org.zicat.tributary.demo.sink;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.zicat.tributary.common.BytesUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/** HDFSSinkParquetReader. */
public class HDFSSinkParquetReader {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException, URISyntaxException {
        final Configuration configuration = new Configuration();
        final URI uri =
                Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResource(
                                                "7420ae3d_8f57_4105_9875_3ca0a495b003_c1_group_1_0.1.snappy.parquet"))
                        .toURI();
        final InputFile inputFile =
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(uri), configuration);

        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(inputFile)
                        .withConf(configuration)
                        .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                final String topic = record.get("topic").toString();
                final int partition = (int) record.get("partition");
                final ByteBuffer key = (ByteBuffer) record.get("key");
                final ByteBuffer value = (ByteBuffer) record.get("value");
                final Map<Utf8, ByteBuffer> headers = (Map<Utf8, ByteBuffer>) record.get("headers");
                final StringJoiner sj = new StringJoiner(", ");
                headers.forEach((k, v) -> sj.add(k.toString() + ":" + BytesUtils.toString(v)));
                System.out.println(
                        "topic:"
                                + topic
                                + ", partition:"
                                + partition
                                + ", headers:["
                                + sj
                                + "], key:"
                                + new String(key.array())
                                + ", value:"
                                + new String(value.array()));
            }
        }
    }
}
