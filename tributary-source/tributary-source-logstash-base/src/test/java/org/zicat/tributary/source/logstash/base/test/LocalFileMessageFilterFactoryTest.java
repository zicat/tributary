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

package org.zicat.tributary.source.logstash.base.test;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ResourceUtils;
import org.zicat.tributary.source.logstash.base.LocalFileMessageFilterFactory;
import org.zicat.tributary.source.logstash.base.Message;
import org.zicat.tributary.source.logstash.base.MessageFilterFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** LocalFileMessageFilterFactoryTest. */
public class LocalFileMessageFilterFactoryTest {
    private static final File PATH1 =
            new File(ResourceUtils.getResourcePath("DefaultMessageFilter.txt"));
    private static final File PATH2 =
            new File(ResourceUtils.getResourcePath("DefaultMessageFilter2.txt"));

    private static final File CHANGEABLE_PATH = new File(PATH1.getParentFile(), "changeable.txt");

    @BeforeClass
    public static void before() throws IOException {
        if (CHANGEABLE_PATH.exists()) {
            if (!CHANGEABLE_PATH.delete()) {
                throw new RuntimeException("delete file failed");
            }
        }
        if (!CHANGEABLE_PATH.createNewFile()) {
            throw new RuntimeException("create file failed");
        }
    }

    @Test
    public void test() throws Exception {
        LocalFileMessageFilterFactory messageFilterFactory = null;
        try {
            messageFilterFactory =
                    (LocalFileMessageFilterFactory)
                            MessageFilterFactory.findFactory(
                                    LocalFileMessageFilterFactory.IDENTITY);
            DefaultReadableConfig config = new DefaultReadableConfig();
            config.put(
                    LocalFileMessageFilterFactory.OPTION_LOCAL_FILE_PATH,
                    CHANGEABLE_PATH.getPath());
            IOUtils.transform(PATH1, CHANGEABLE_PATH);
            messageFilterFactory.open(config);

            final Map<String, Object> data = new HashMap<>();
            data.put("aa", "111");
            data.put("bb", "222");
            Assert.assertNotNull(
                    messageFilterFactory.getMessageFilter().convert("", new Message<>(1, data)));
            Assert.assertNull(data.get("aa"));
            Assert.assertEquals(1, data.size());

            if (!CHANGEABLE_PATH.delete()) {
                throw new RuntimeException("delete file failed");
            }
            IOUtils.transform(PATH2, CHANGEABLE_PATH);
            Thread.sleep(1500);

            final Map<String, Object> data2 = new HashMap<>();
            data2.put("aa", "111");
            data2.put("bb", "222");
            Assert.assertNotNull(
                    messageFilterFactory.getMessageFilter().convert("", new Message<>(1, data2)));
            Assert.assertNull(data2.get("bb"));
            Assert.assertEquals(1, data2.size());

        } finally {
            IOUtils.closeQuietly(messageFilterFactory);
        }
        Assert.assertTrue(messageFilterFactory.isClosed());
    }

    @AfterClass
    public static void after() {
        if (CHANGEABLE_PATH.exists()) {
            if (!CHANGEABLE_PATH.delete()) {
                throw new RuntimeException("delete file failed");
            }
        }
    }
}
