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

import java.util.HashMap;
import java.util.Map;

/** ConfigBuilder. */
@SuppressWarnings("unchecked")
public abstract class ConfigBuilder<T extends ConfigBuilder<T, V>, V extends ReadableConfig> {

    protected final Map<String, Object> config = new HashMap<>();

    /**
     * add property.
     *
     * @param key key
     * @param value value
     * @return this
     */
    public final T addConfig(String key, Object value) {
        config.put(key, value);
        return (T) this;
    }

    /**
     * add custom properties.
     *
     * @param option option
     * @param value value
     * @return this
     */
    public final <X> T addConfig(ConfigOption<X> option, X value) {
        final Object realValue =
                option.hasDefaultValue() && value == null ? option.defaultValue() : value;
        return addConfig(option.key(), realValue);
    }

    /**
     * add all properties.
     *
     * @param value values.
     * @return this
     */
    public final T addConfigs(Map<String, Object> value) {
        config.putAll(value);
        return (T) this;
    }

    /**
     * add property if contain input key.
     *
     * @param containsKey containsKey
     * @param key key
     * @param value value
     * @return this
     */
    public final T addConfigIfContainKey(String containsKey, String key, Object value) {
        if (config.containsKey(containsKey)) {
            return addConfig(key, value);
        }
        return (T) this;
    }

    /**
     * build config.
     *
     * @return config
     */
    public abstract V build();
}
