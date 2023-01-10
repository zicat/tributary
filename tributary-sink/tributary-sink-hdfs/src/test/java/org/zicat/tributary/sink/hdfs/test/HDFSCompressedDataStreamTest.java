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

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.queue.utils.IOUtils;
import org.zicat.tributary.sink.hdfs.HDFSCompressedDataStream;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;

/** HDFSCompressedDataStreamTest. */
public class HDFSCompressedDataStreamTest {

    private static final Logger logger =
            LoggerFactory.getLogger(HDFSCompressedDataStreamTest.class);

    @Before
    @After
    public void after() {
        IOUtils.deleteDir(new File("/tmp/compression_test"));
    }

    @Test
    public void testSnappy() throws Exception {

        File file = new File("/tmp/compression_test/foo2.gz");

        String fileURI = file.getAbsoluteFile().toURI().toString();
        logger.info("File URI: {}", fileURI);

        Configuration conf = new Configuration();
        FileSystem fileSystem = new RawLocalFileSystem();
        fileSystem.setConf(conf);
        final Path path = new Path(fileURI);

        SnappyCodec snappyCodec = new SnappyCodec();
        snappyCodec.setConf(conf);
        final HDFSCompressedDataStream writer = new HDFSCompressedDataStream();
        writer.open(fileSystem, path, snappyCodec);

        final String[] bodies = {"yarf!"};
        writeBodies(writer, bodies);

        final byte[] buf = new byte[256];

        final CompressionInputStream cmpIn =
                snappyCodec.createInputStream(new FileInputStream(file));
        int len = cmpIn.read(buf);
        String result = new String(buf, 0, len, Charsets.UTF_8);
        result = result.trim(); // BodyTextEventSerializer adds a newline

        Assert.assertEquals("input and output must match", bodies[0], result);

        writeBodies(writer, bodies);
        writer.close();
        len = cmpIn.read(buf);
        result = new String(buf, 0, len, Charsets.UTF_8);
        result = result.trim(); // BodyTextEventSerializer adds a newline
        Assert.assertEquals("input and output must match", bodies[0], result);
    }

    /**
     * write bodies.
     *
     * @param writer writer
     * @param bodies bodies
     * @throws Exception Exception
     */
    private void writeBodies(HDFSCompressedDataStream writer, String... bodies) throws Exception {
        for (String body : bodies) {
            writer.append(body.getBytes(StandardCharsets.UTF_8));
        }
        writer.sync();
        writer.sync();
    }
}
