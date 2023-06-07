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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.SnappyCodec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** HDFSSinkFileReader. */
public class HDFSSinkSnappyFileReader {

    public static void main(String[] args) throws IOException {
        final String fileName = "2b97417e_bceb_48b5_8f08_f299426bb56b_c1_group_1_0.1.snappy";
        SnappyCodec snappyCodec = new SnappyCodec();
        snappyCodec.setConf(new Configuration());
        final byte[] lengthBytes = new byte[4];
        try (CompressionInputStream compressionInputStream =
                snappyCodec.createInputStream(
                        Thread.currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream(fileName))) {
            int readCount;
            while ((readCount = readAll(compressionInputStream, lengthBytes)) != -1) {
                if (readCount != lengthBytes.length) {
                    throw new IOException("error file schema");
                }
                final int length = ByteBuffer.wrap(lengthBytes).getInt();
                final byte[] body = new byte[length];
                if (readAll(compressionInputStream, body) != length) {
                    throw new IOException("read body fail");
                }
                System.out.println(new String(body, StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * read InputStream to bytes.
     *
     * @param is is
     * @param bytes bytes
     * @return return false, else return true.
     * @throws IOException IOException
     */
    public static int readAll(InputStream is, byte[] bytes) throws IOException {
        return readAll(is, bytes, 0, bytes.length);
    }

    /**
     * read InputStream to bytes.
     *
     * @param is is
     * @param bytes bytes
     * @param sourceOffset bytes offset
     * @param length bytes length
     * @return return -1 if eof, else return real read count.
     * @throws IOException IOException
     */
    public static int readAll(InputStream is, byte[] bytes, int sourceOffset, int length)
            throws IOException {
        int offset = sourceOffset;
        do {
            final int remaining = length - offset;
            final int readCount = is.read(bytes, offset, remaining);
            if (readCount == -1) {
                return offset == sourceOffset ? -1 : offset - sourceOffset;
            }
            if (readCount == remaining) {
                return length;
            }
            if (readCount > remaining) {
                throw new IOException("read overflow");
            }
            offset += readCount;
        } while (true);
    }
}
