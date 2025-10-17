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

package org.zicat.tributary.common.config;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;

/** ReadableConfig. */
public interface ReadableConfig {

    /**
     * group by keyHandler.
     *
     * @param keyHandler keyHandler
     * @return set
     */
    Set<String> groupKeys(KeyHandler keyHandler);

    /**
     * forEach.
     *
     * @param consumer consumer
     */
    void forEach(BiConsumer<? super String, ? super Object> consumer);

    /**
     * create with values.
     *
     * @param values values
     * @return Readable Config
     */
    static ReadableConfig create(Map<String, Object> values) {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        if (values != null) {
            config.putAll(values);
        }
        return config;
    }

    /**
     * get value by config option.
     *
     * @param configOption configOption
     * @param <T> type of value
     * @return value
     */
    <T> T get(ConfigOption<T> configOption);

    /**
     * get value by config option, if null return defaultValue.
     *
     * @param configOption configOption
     * @param defaultValue defaultValue
     * @return value
     * @param <T> T
     */
    default <T> T get(ConfigOption<T> configOption, T defaultValue) {
        try {
            final T t = get(configOption);
            return t == null ? defaultValue : t;
        } catch (Exception ignore) {
            return defaultValue;
        }
    }

    /**
     * get value by config option, if null return defaultValue.
     *
     * @param configOption configOption
     * @param defaultValue defaultValue
     * @return value
     * @param <T> T
     */
    default <T> T get(ConfigOption<T> configOption, ConfigOption<T> defaultValue) {
        try {
            final T t = get(configOption);
            return t == null ? get(defaultValue) : t;
        } catch (Exception e) {
            return get(defaultValue);
        }
    }

    /**
     * filter and remove prefix key.
     *
     * @param prefixKey prefixKey
     * @return ReadableConfig
     */
    ReadableConfig filterAndRemovePrefixKey(String prefixKey);

    /**
     * to properties.
     *
     * @return properties.
     */
    Properties toProperties();

    /** KeyHandler. */
    interface KeyHandler {

        /**
         * get want key.
         *
         * @param key key
         * @return value
         */
        String apply(String key);
    }

    /** first key split by . */
    class FirstKey implements KeyHandler {

        private final String split;

        public FirstKey(String split) {
            this.split = split;
        }

        @Override
        public String apply(String key) {
            return key.split(split)[0];
        }
    }

    ReadableConfig.KeyHandler DEFAULT_KEY_HANDLER = new ReadableConfig.FirstKey("\\.");
}
