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

package org.zicat.tributary.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/** Config. */
public class Config {

    protected static final Logger LOG = LoggerFactory.getLogger(Config.class);
    protected final Map<String, Object> customConfig;

    protected Config(Map<String, Object> customConfig) {
        this.customConfig = Collections.unmodifiableMap(customConfig);
    }

    /**
     * get custom property.
     *
     * @param key key
     * @param defaultValue defaultValue if not contains.
     * @param converter converter
     * @param <T> T
     * @return T
     */
    public <T> T getCustomProperty(String key, T defaultValue, ObjectConverter<T> converter) {
        final Object value = getCustomProperty(key);
        if (value == null) {
            LOG.info("{} not found, use default value {}", key, defaultValue);
            return defaultValue;
        }
        try {
            return converter.toValue(value);
        } catch (Exception e) {
            LOG.warn(
                    "parser {} params as error, value = {}, use default value {}",
                    key,
                    value,
                    defaultValue);
            return defaultValue;
        }
    }

    /**
     * get string property value.
     *
     * @param key key
     * @param defaultValue defaultValue
     * @return string value
     */
    public String getCustomProperty(String key, String defaultValue) {
        return getCustomProperty(key, defaultValue, Object::toString);
    }

    /**
     * get int property.
     *
     * @param key key
     * @param defaultValue defaultValue
     * @return value
     */
    public int getCustomProperty(String key, int defaultValue) {
        return getCustomProperty(key, defaultValue, t -> Integer.parseInt(t.toString()));
    }

    /**
     * get long property.
     *
     * @param key key
     * @param defaultValue defaultValue
     * @return value
     */
    public long getCustomProperty(String key, long defaultValue) {
        return getCustomProperty(key, defaultValue, t -> Long.parseLong(t.toString()));
    }

    /**
     * get custom config.
     *
     * @return map
     */
    public final Map<String, Object> customConfig() {
        return customConfig;
    }

    /**
     * get property value.
     *
     * @param key key
     * @return object value
     */
    public Object getCustomProperty(String key) {
        return customConfig.get(key);
    }

    /**
     * get all properties by prefix key.
     *
     * <p>note: the key in property will be removed the prefix,
     *
     * <p>e.g. prefix = 'aa', key = 'aa.bb' value = 'cc', properties key = 'bb' value 'cc'
     *
     * @param prefix prefix key
     * @return properties
     */
    public Properties filterPropertyByPrefix(String prefix) {
        final Properties properties = new Properties();
        for (Map.Entry<String, Object> entry : customConfig.entrySet()) {
            final String key = entry.getKey();
            final int index = key.indexOf(prefix);
            if (index == 0) {
                if (entry.getValue() instanceof String) {
                    properties.setProperty(
                            key.substring(prefix.length()), (String) entry.getValue());
                } else {
                    throw new IllegalStateException(
                            "only support string property value, key = " + key);
                }
            }
        }
        return properties;
    }

    /**
     * ObjectConverter.
     *
     * @param <T>
     */
    public interface ObjectConverter<T> {

        /**
         * obj to t.
         *
         * @param t t is not null
         * @return t
         */
        T toValue(Object t);
    }
}
