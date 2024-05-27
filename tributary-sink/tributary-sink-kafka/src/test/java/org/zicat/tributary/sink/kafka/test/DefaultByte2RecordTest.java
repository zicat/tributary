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

package org.zicat.tributary.sink.kafka.test;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.sink.kafka.DefaultByte2Record;

import java.util.List;

/** DefaultByte2RecordTest. */
public class DefaultByte2RecordTest {

    @Test
    public void test() {
        final DefaultByte2Record byte2Record = new DefaultByte2Record("t1");
        final List<ProducerRecord<byte[], byte[]>> records = byte2Record.convert("test".getBytes());
        Assert.assertEquals(1, records.size());

        final ProducerRecord<byte[], byte[]> record = records.get(0);
        Assert.assertEquals("t1", record.topic());
        Assert.assertArrayEquals("test".getBytes(), record.value());
        Assert.assertNull(record.partition());
    }
}
