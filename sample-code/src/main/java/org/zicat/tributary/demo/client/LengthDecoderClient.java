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

package org.zicat.tributary.demo.client;

import org.zicat.tributary.common.exception.TributaryIOException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static org.zicat.tributary.common.util.IOUtils.writeData;

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
            throw new TributaryIOException("channel " + address + " closed");
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
