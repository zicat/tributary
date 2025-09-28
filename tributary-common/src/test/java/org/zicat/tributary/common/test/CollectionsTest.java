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
import org.zicat.tributary.common.Collections;

import java.util.Arrays;
import java.util.Iterator;

/** CollectionsTest. */
public class CollectionsTest {

    @Test
    public void testDeepCopy() {
        Assert.assertNull(Collections.deepCopy(null));

        final Iterator<String> it = Arrays.asList("a", "b").iterator();
        final Iterator<String> copy = Collections.deepCopy(it);
        while (it.hasNext()) {
            Assert.assertEquals(it.next(), copy.next());
        }
        Assert.assertNotSame(it, copy);
    }
}
