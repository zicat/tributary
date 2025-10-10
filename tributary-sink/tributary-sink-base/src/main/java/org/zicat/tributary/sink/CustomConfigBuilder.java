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

import org.zicat.tributary.common.ConfigOption;

import java.util.HashMap;
import java.util.Map;

/** CustomConfigBuilder. */
public class CustomConfigBuilder {

    protected final Map<String, Object> customConfig = new HashMap<>();

    /**
     * add property.
     *
     * @param key key
     * @param value value
     * @return this
     */
    public final CustomConfigBuilder addCustomProperty(String key, Object value) {
        customConfig.put(key, value);
        return this;
    }

    /**
     * add custom properties.
     *
     * @param option option
     * @param value value
     * @return this
     * @param <T> T
     */
    public final <T> CustomConfigBuilder addCustomProperty(ConfigOption<T> option, Object value) {
        final Object realValue =
                option.hasDefaultValue() && value == null ? option.defaultValue() : value;
        return addCustomProperty(option.key(), realValue);
    }

    /**
     * add all properties.
     *
     * @param value values.
     * @return this
     */
    public final CustomConfigBuilder addAll(Map<String, Object> value) {
        customConfig.putAll(value);
        return this;
    }

    /**
     * add property if contain input key.
     *
     * @param containsKey containsKey
     * @param key key
     * @param value value
     * @return this
     */
    public final CustomConfigBuilder addCustomPropertyIfContainKey(
            String containsKey, String key, Object value) {
        if (customConfig.containsKey(containsKey)) {
            return addCustomProperty(key, value);
        }
        return this;
    }
}
