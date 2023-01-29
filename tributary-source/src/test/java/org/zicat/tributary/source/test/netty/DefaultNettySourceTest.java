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

package org.zicat.tributary.source.test.netty;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.MockChannel;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.source.netty.DefaultNettySource;
import org.zicat.tributary.source.netty.client.LengthDecoderClient;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

/** DefaultNettySourceTest. */
public class DefaultNettySourceTest {

    @Test
    public void test() throws InterruptedException, IOException {
        final MockChannel channel = new MockChannel();
        final int port = getFreeTcpPort();
        try (DefaultNettySource server = new DefaultNettySource(port, channel)) {
            server.listen();

            final byte[] data1 = "lyn".getBytes();
            final byte[] data2 = "zhangjun".getBytes();
            try (LengthDecoderClient client = new LengthDecoderClient(port)) {
                Assert.assertEquals(data1.length, client.append(data1));
                Assert.assertEquals(data2.length, client.append(data2));
            }

            RecordsResultSet recordsResultSet =
                    channel.poll(0, new RecordsOffset(0, 0), 10, TimeUnit.MILLISECONDS);
            Assert.assertArrayEquals(data1, recordsResultSet.next());

            RecordsResultSet recordsResultSet2 =
                    channel.poll(0, recordsResultSet.nexRecordsOffset(), 10, TimeUnit.MILLISECONDS);
            Assert.assertArrayEquals(data2, recordsResultSet2.next());
        }
    }

    /**
     * get free tcp port .
     *
     * @return port
     * @throws IOException IOException
     */
    public static int getFreeTcpPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }
}
