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

package org.zicat.tributary.channel.test;

import static org.zicat.tributary.common.util.BytesUtils.toBytes;
import static org.zicat.tributary.common.records.RecordsUtils.createBytesRecords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.exception.TributaryRuntimeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Random;

/** ConsumerThread. */
public class SourceThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SourceThread.class);

    public static Random random = new Random(123123123L);

    final Channel channel;
    final int partition;
    final long dataSize;
    final byte[] data;
    final int halfMax;
    final boolean realRandom;

    public SourceThread(
            Channel channel,
            int partition,
            long dataSize,
            int recordMaxLength,
            boolean realRandom) {
        this.channel = channel;
        this.partition = partition;
        this.dataSize = dataSize;
        this.halfMax = recordMaxLength / 2;
        this.realRandom = realRandom;
        data = new byte[recordMaxLength];
        Random realR = getRandom();
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) realR.nextInt();
        }
    }

    public SourceThread(Channel channel, int partition, long dataSize, int recordMaxLength) {
        this(channel, partition, dataSize, recordMaxLength, false);
    }

    @Override
    public void run() {
        long writeLength = 0;
        long start = System.currentTimeMillis();
        DecimalFormat df = new DecimalFormat("######0.00");
        Random realR = getRandom();
        for (int id = 0; id < dataSize; id++) {
            try {
                int length = realR.nextInt(halfMax) + halfMax;
                channel.append(
                        partition,
                        createBytesRecords(
                                        channel.topic(), toBytes(ByteBuffer.wrap(data, 0, length)))
                                .toByteBuffer());
                writeLength += length;
                long spend = System.currentTimeMillis() - start;
                if (writeLength >= 1024 * 1024 * 1024 && spend > 0) {
                    LOG.info(
                            "write spend:{}(mb/s)",
                            df.format(writeLength / 1024.0 / 1024.0 / (spend / 1000.0)));
                    writeLength = 0;
                    start = System.currentTimeMillis();
                }
            } catch (IOException | InterruptedException e) {
                throw new TributaryRuntimeException(e);
            }
        }
        LOG.info("SourceThread finished, partition:{}, count:{}", partition, dataSize);
    }

    private Random getRandom() {
        return realRandom ? new Random(System.currentTimeMillis()) : random;
    }
}
