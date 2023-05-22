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

package org.zicat.tributary.sink.test.function;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.sink.function.SystemClock;

/** SystemClockTest. */
public class SystemClockTest {

    @Test
    public void testTimeFormat() {
        final long time = 1684726202000L;
        final String utc = "2023-05-22 03:30:02";
        final String gmt8 = "2023-05-22 11:30:02";
        final String gmt22 = "2023-05-23 01:30:02";
        Assert.assertEquals(utc, SystemClock.timeFormat(time, "yyyy-MM-dd HH:mm:ss", "UTC"));
        Assert.assertEquals(gmt8, SystemClock.timeFormat(time, "yyyy-MM-dd HH:mm:ss", "GMT+8"));
        Assert.assertEquals(gmt22, SystemClock.timeFormat(time, "yyyy-MM-dd HH:mm:ss", "GMT+22"));
    }
}
