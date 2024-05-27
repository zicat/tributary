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

package org.zicat.tributary.common.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.BytesUtils;

import java.nio.ByteBuffer;

/** BytesUtilsTest. */
public class BytesUtilsTest {

    @Test
    public void testToBytesInt() {

        Assert.assertEquals(0, BytesUtils.toInt(BytesUtils.toBytes(0)));
        Assert.assertEquals(-10, BytesUtils.toInt(BytesUtils.toBytes(-10)));
        Assert.assertEquals(10, BytesUtils.toInt(BytesUtils.toBytes(10)));
        Assert.assertEquals(
                Integer.MIN_VALUE, BytesUtils.toInt(BytesUtils.toBytes(Integer.MIN_VALUE)));
        Assert.assertEquals(
                Integer.MAX_VALUE, BytesUtils.toInt(BytesUtils.toBytes(Integer.MAX_VALUE)));
    }

    @Test
    public void testToString() {
        final String s = "foo";
        Assert.assertEquals(s, BytesUtils.toString(ByteBuffer.wrap(s.getBytes())));
    }
}
