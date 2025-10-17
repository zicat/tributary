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

package org.zicat.tributary.common.test.config;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.config.ConfigOptions;
import static org.zicat.tributary.common.config.ConfigOptions.COMMA_SPLIT_HANDLER;
import org.zicat.tributary.common.config.DefaultReadableConfig;
import org.zicat.tributary.common.config.PercentSize;

/** ReadableConfigTest. */
public class ReadableConfigTest {

    @Test
    public void test() {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        Assert.assertEquals(
                "bb", config.get(ConfigOptions.key("aa").stringType().defaultValue(null), "bb"));
        Assert.assertEquals(
                "bb", config.get(ConfigOptions.key("aa").stringType().noDefaultValue(), "bb"));

        try {
            config.get(ConfigOptions.key("aa").stringType().noDefaultValue());
            Assert.fail();
        } catch (Exception ignore) {
        }

        config.put("aa", "bb");
        Assert.assertEquals(
                "bb", config.get(ConfigOptions.key("aa").stringType().noDefaultValue()));
        Assert.assertEquals(
                "bb", config.get(ConfigOptions.key("aa").stringType().defaultValue("hh")));

        Assert.assertEquals(
                "dd", config.get(ConfigOptions.key("cc").stringType().defaultValue("dd")));
    }

    @Test
    public void testListType() {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put("aa", "aa,bb;,,cc");
        Assert.assertArrayEquals(
                new String[] {"aa", "bb;", "cc"},
                config.get(ConfigOptions.key("aa").listType(COMMA_SPLIT_HANDLER).noDefaultValue())
                        .toArray(new String[] {}));
    }

    @Test
    public void testPercentType() {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put("aa", "10.1 % ");
        config.put("bb", "20%");
        Assert.assertEquals(
                10.1,
                config.get(ConfigOptions.key("aa").percentType().noDefaultValue()).getPercent(),
                0.01);

        Assert.assertEquals(
                20,
                config.get(ConfigOptions.key("bb").percentType().noDefaultValue()).getPercent(),
                0);

        try {
            config.get(ConfigOptions.key("cc").percentType().noDefaultValue());
            Assert.fail();
        } catch (Exception ignore) {

        }
        Assert.assertEquals(
                35,
                config.get(ConfigOptions.key("dd").percentType().defaultValue(new PercentSize(35)))
                        .getPercent(),
                0);
        Assert.assertEquals(
                10.1,
                config.get(ConfigOptions.key("aa").percentType().defaultValue(new PercentSize(35)))
                        .getPercent(),
                0.01);
        try {
            new PercentSize(101);
            Assert.fail();
        } catch (Exception ignore) {

        }
        try {
            new PercentSize(-1);
            Assert.fail();
        } catch (Exception ignore) {

        }
    }

    @Test
    public void testEnumType() {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put("aa", "none");
        config.put("bb", "zstd");
        config.put("cc", "snappy");
        config.put("dd", "Zstd");
        config.put("ee", "ZSTD");
        Assert.assertEquals(
                CompressionTypeMock.NONE,
                config.get(
                        ConfigOptions.key("aa")
                                .enumType(CompressionTypeMock.class)
                                .noDefaultValue()));
        Assert.assertEquals(
                CompressionTypeMock.ZSTD,
                config.get(
                        ConfigOptions.key("bb")
                                .enumType(CompressionTypeMock.class)
                                .noDefaultValue()));
        Assert.assertEquals(
                CompressionTypeMock.SNAPPY,
                config.get(
                        ConfigOptions.key("cc")
                                .enumType(CompressionTypeMock.class)
                                .noDefaultValue()));

        Assert.assertEquals(
                CompressionTypeMock.ZSTD,
                config.get(
                        ConfigOptions.key("dd")
                                .enumType(CompressionTypeMock.class)
                                .noDefaultValue()));
        Assert.assertEquals(
                CompressionTypeMock.ZSTD,
                config.get(
                        ConfigOptions.key("ee")
                                .enumType(CompressionTypeMock.class)
                                .noDefaultValue()));
    }

    @Test
    public void testDurationType() {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put("aa", "10ms");
        config.put("bb", "20sec");
        config.put("cc", "30min");
        config.put("dd", "40h");
        config.put("ee", "50d");
        Assert.assertEquals(
                10L,
                config.get(ConfigOptions.key("aa").durationType().noDefaultValue()).toMillis());
        Assert.assertEquals(
                20 * 1000L,
                config.get(ConfigOptions.key("bb").durationType().noDefaultValue()).toMillis());
        Assert.assertEquals(
                30 * 60 * 1000L,
                config.get(ConfigOptions.key("cc").durationType().noDefaultValue()).toMillis());
        Assert.assertEquals(
                40 * 60 * 60 * 1000L,
                config.get(ConfigOptions.key("dd").durationType().noDefaultValue()).toMillis());
        Assert.assertEquals(
                50 * 24 * 60 * 60 * 1000L,
                config.get(ConfigOptions.key("ee").durationType().noDefaultValue()).toMillis());
    }

    @Test
    public void testMemoryType() {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put("aa", "10b");
        config.put("bb", "20kb");
        config.put("cc", "30mb");
        config.put("dd", "40gb");
        config.put("ee", "50tb");
        Assert.assertEquals(
                10L, config.get(ConfigOptions.key("aa").memoryType().noDefaultValue()).getBytes());
        Assert.assertEquals(
                20 * 1024L,
                config.get(ConfigOptions.key("bb").memoryType().noDefaultValue()).getBytes());
        Assert.assertEquals(
                30 * 1024 * 1024L,
                config.get(ConfigOptions.key("cc").memoryType().noDefaultValue()).getBytes());
        Assert.assertEquals(
                40 * 1024 * 1024 * 1024L,
                config.get(ConfigOptions.key("dd").memoryType().noDefaultValue()).getBytes());
        Assert.assertEquals(
                50 * 1024 * 1024 * 1024L * 1024L,
                config.get(ConfigOptions.key("ee").memoryType().noDefaultValue()).getBytes());
    }

    /** CompressionTypeMock. */
    public enum CompressionTypeMock {
        NONE((byte) 1, "none"),
        ZSTD((byte) 2, "zstd"),
        SNAPPY((byte) 3, "snappy");

        private final byte id;
        private final String name;

        CompressionTypeMock(byte id, String name) {
            this.id = id;
            this.name = name;
        }

        public byte id() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
