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

import static java.util.Collections.singletonMap;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.Records;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import org.zicat.tributary.common.records.SingleRecords;

import java.util.concurrent.atomic.AtomicBoolean;

/** RecordsUtilsTest. */
public class RecordsUtilsTest {

    private static final String HEADER_KEY_REC_TS = "rec_ts";

    @Test
    public void testForeachRecord() throws Exception {
        final Records records =
                new SingleRecords(
                        "t1",
                        singletonMap(HEADER_KEY_REC_TS, "123".getBytes()),
                        new DefaultRecord(
                                singletonMap(HEADER_KEY_REC_TS, "124".getBytes()),
                                "v1".getBytes()));
        final AtomicBoolean accepted = new AtomicBoolean();
        foreachRecord(
                records,
                (key, value, allHeaders) -> {
                    Assert.assertEquals("v1", new String(value));
                    Assert.assertEquals("124 123", new String(allHeaders.get(HEADER_KEY_REC_TS)));
                    accepted.set(true);
                });
        Assert.assertTrue(accepted.get());
    }
}
