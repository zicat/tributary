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

package org.zicat.tributary.service.source.netty.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

/** LengthDecoderClient. */
public class LengthDecoderClient implements Closeable {

    private final InetSocketAddress address;
    private SocketChannel socketChannel;

    public LengthDecoderClient(int port) throws IOException {
        this(new InetSocketAddress(port));
    }

    public LengthDecoderClient(String host, int port) throws IOException {
        this(new InetSocketAddress(host, port));
    }

    public LengthDecoderClient(InetSocketAddress address) throws IOException {
        this.address = address;
        this.socketChannel = SocketChannel.open(address);
    }

    /**
     * append record.
     *
     * @param record record
     * @return response length
     * @throws IOException IOException
     */
    public int append(byte[] record) throws IOException {
        if (!isOpen()) {
            throw new IOException("channel " + address + " closed");
        }
        return writeData(socketChannel, record);
    }

    /**
     * check client is open.
     *
     * @return boolean
     */
    private boolean isOpen() {
        return socketChannel != null;
    }

    /**
     * write data to channel.
     *
     * @param socketChannel socketChannel
     * @param data data
     * @throws IOException IOException
     */
    public static int writeData(SocketChannel socketChannel, byte[] data) throws IOException {
        final ByteBuffer len =
                ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(data.length);
        len.flip();
        writeToChannel(socketChannel, len);
        writeToChannel(socketChannel, ByteBuffer.wrap(data));
        final ByteBuffer response = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        readFromChannel(socketChannel, response);
        return response.getInt();
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
    public static void readFromChannel(ReadableByteChannel fileChannel, ByteBuffer byteBuffer)
            throws IOException {
        while (byteBuffer.hasRemaining()) {
            fileChannel.read(byteBuffer);
        }
        byteBuffer.flip();
    }

    @Override
    public void close() throws IOException {
        if (isOpen()) {
            try {
                socketChannel.close();
            } finally {
                socketChannel = null;
            }
        }
    }
}