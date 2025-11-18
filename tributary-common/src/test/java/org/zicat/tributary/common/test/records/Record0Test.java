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

package org.zicat.tributary.common.test.records;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.Record0;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/** RecordTest. */
public class Record0Test {

    @Test
    public void test() {
        test(new DefaultRecord(null, null, null));

        final Map<String, byte[]> headers = new HashMap<>();
        headers.put("h1", "hv1".getBytes());
        headers.put("h2", "hv2".getBytes());
        test(new DefaultRecord(headers, "k1".getBytes(), "k2".getBytes()));
    }

    public static void test(Record0 record) {
        final ByteBuffer buffer = record.toByteBuffer();
        Assert.assertEquals(0, buffer.position());
        Assert.assertEquals(buffer.limit(), buffer.capacity());
        assertRecord(record, Record0.parse(buffer));
    }

    public static void assertRecord(Record0 record, Record0 newRecord) {
        Assert.assertArrayEquals(record.key(), newRecord.key());
        Assert.assertArrayEquals(record.value(), newRecord.value());
        Assert.assertEquals(record.headers().size(), newRecord.headers().size());
        for (String key : record.headers().keySet()) {
            Assert.assertArrayEquals(record.headers().get(key), newRecord.headers().get(key));
        }
    }
}
