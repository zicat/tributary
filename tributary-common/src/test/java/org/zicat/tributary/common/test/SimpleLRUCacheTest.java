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
import org.zicat.tributary.common.SimpleLRUCache;

import java.util.Map;

/** SimpleLRUCacheTest. */
public class SimpleLRUCacheTest {

    @Test
    public void testPutOrder() {
        Map<String, Integer> cache = SimpleLRUCache.create(2);
        cache.put("1", 1);
        cache.put("2", 2);
        Assert.assertEquals(1, cache.get("1").intValue());
        cache.put("3", 3);
        Assert.assertNull(cache.get("1"));
        Assert.assertEquals(2, cache.get("2").intValue());
        Assert.assertEquals(3, cache.get("3").intValue());
        Assert.assertEquals(2, cache.size());
    }

    @Test
    public void testGetOrder() {
        Map<String, Integer> cache = SimpleLRUCache.create(2, true);
        cache.put("1", 1);
        cache.put("2", 2);
        Assert.assertEquals(1, cache.get("1").intValue());
        cache.put("3", 3);
        Assert.assertNull(cache.get("2"));
        Assert.assertEquals(2, cache.size());
        Assert.assertEquals(1, cache.get("1").intValue());
        Assert.assertEquals(3, cache.get("3").intValue());
    }
}
