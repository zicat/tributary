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
import org.zicat.tributary.common.VIntUtil;

import java.nio.ByteBuffer;

/** VIntUtilTest. */
public class VIntUtilTest {

    @Test
    public void test() {
        for (int i = 0; i < 4; i++) {
            final int offset = i + 1;
            final int a = (1 << (7 * offset));
            final int b = a - 1;
            final int c = a + 1;
            Assert.assertEquals(offset + 1, VIntUtil.vIntLength(a) - a);
            Assert.assertEquals(offset, VIntUtil.vIntLength(b) - b);
            Assert.assertEquals(offset + 1, VIntUtil.vIntLength(c) - c);
            testReadPutVint(a);
            testReadPutVint(b);
            testReadPutVint(c);
        }
    }

    @Test
    public void testFail() {
        try {
            Assert.fail(String.valueOf(VIntUtil.vIntLength(-1)));
        } catch (Exception ignore) {
            Assert.assertTrue(true);
        }
    }

    /**
     * test vint i.
     *
     * @param i i
     */
    private void testReadPutVint(int i) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(5);
        VIntUtil.putVInt(byteBuffer, i);
        byteBuffer.flip();
        Assert.assertEquals(i, VIntUtil.readVInt(byteBuffer));
    }
}
