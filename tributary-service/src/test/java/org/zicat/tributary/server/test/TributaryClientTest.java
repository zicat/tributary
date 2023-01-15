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

package org.zicat.tributary.server.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/** TributaryClientTest. */
public class TributaryClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(TributaryClientTest.class);
    private static final InetSocketAddress LOCAL = new InetSocketAddress("localhost", 8300);
    private static final Random random = new Random();

    public static void main(String[] args) throws IOException {

        try (SocketChannel socketChannel = SocketChannel.open(LOCAL)) {
            for (int i = 0; i < 1; i++) {
                createTestDate(socketChannel);
            }
        }
    }

    /**
     * create test data.
     *
     * @param socketChannel socketChannel
     * @throws IOException IOException
     */
    private static void createTestDate(SocketChannel socketChannel) throws IOException {
        int length = random.nextInt(100) + 50;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(i % 2 == 0 ? "a" : "b");
        }
        writeData(socketChannel, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * write data to channel.
     *
     * @param socketChannel socketChannel
     * @param data data
     * @throws IOException IOException
     */
    private static void writeData(SocketChannel socketChannel, byte[] data) throws IOException {
        final ByteBuffer len =
                ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(data.length);
        len.flip();
        writeToChannel(socketChannel, len);
        writeToChannel(socketChannel, ByteBuffer.wrap(data));
        final ByteBuffer response = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        readFromChannel(socketChannel, response);
        LOG.info("send length:{}, response length:{}", data.length, response.getInt());
    }

    /**
     * write to channel.
     *
     * @param socketChannel socketChannel
     * @param byteBuffer byteBuffer
     * @throws IOException IOException
     */
    public static void writeToChannel(WritableByteChannel socketChannel, ByteBuffer byteBuffer)
            throws IOException {
        while (byteBuffer.hasRemaining()) {
            socketChannel.write(byteBuffer);
        }
    }

    /**
     * read from channel.
     *
     * @param fileChannel fileChannel
     * @param byteBuffer byteBuffer
     * @throws IOException IOException
     */
    static void readFromChannel(ReadableByteChannel fileChannel, ByteBuffer byteBuffer)
            throws IOException {
        while (byteBuffer.hasRemaining()) {
            fileChannel.read(byteBuffer);
        }
        byteBuffer.flip();
    }
}
