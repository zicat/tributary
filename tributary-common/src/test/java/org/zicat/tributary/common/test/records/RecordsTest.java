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
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.common.records.Record0;
import org.zicat.tributary.common.records.Records;

import java.nio.ByteBuffer;
import java.util.*;

/** RecordsTest. */
public class RecordsTest {

    @Test
    public void test() {

        try {
            new DefaultRecords(null, null, null);
            Assert.fail();
        } catch (IllegalArgumentException ignore) {
        }

        test(new DefaultRecords("s1", null, null));

        final Map<String, byte[]> headers = new HashMap<>();
        headers.put("h1", "hv1".getBytes());
        headers.put("h2", "hv2".getBytes());

        final Map<String, byte[]> record1Headers = new HashMap<>();
        record1Headers.put("rh1", "rhv1".getBytes());
        record1Headers.put("rh2", "rhv2".getBytes());

        final Map<String, byte[]> record2Headers = new HashMap<>();
        record2Headers.put("rh11", "rhv11".getBytes());
        record2Headers.put("rh22", "rhv22".getBytes());
        final Collection<Record0> records =
                Arrays.asList(
                        new DefaultRecord(record1Headers, "kr1".getBytes(), "vr1".getBytes()),
                        new DefaultRecord(record2Headers, "kr2".getBytes(), "vr2".getBytes()));

        test(new DefaultRecords("s1", headers, records));
    }

    public static void test(Records records) {
        final ByteBuffer buffer = records.toByteBuffer();
        Assert.assertEquals(0, buffer.position());
        Assert.assertEquals(buffer.limit(), buffer.capacity());

        final Records newRecords = Records.parse(buffer);
        Assert.assertEquals(records.topic(), newRecords.topic());
        Assert.assertEquals(records.count(), newRecords.count());

        final Iterator<Record0> it = records.iterator();
        final Iterator<Record0> it2 = records.iterator();
        while (it.hasNext()) {
            Record0Test.assertRecord(it.next(), it2.next());
        }
    }
}
