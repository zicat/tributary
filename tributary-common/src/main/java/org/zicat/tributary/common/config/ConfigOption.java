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

import java.util.function.Function;

/** ConfigOption. */
public class ConfigOption<T> {

    private final String key;
    private final T defaultValue;
    private final String description;
    private final boolean hasDefaultValue;
    private final Function<Object, T> valueConvert;

    ConfigOption(
            String key,
            Function<Object, T> valueConvert,
            T defaultValue,
            String description,
            boolean hasDefaultValue) {
        this.key = key;
        this.valueConvert = valueConvert;
        this.defaultValue = defaultValue;
        this.description = description;
        this.hasDefaultValue = hasDefaultValue;
    }

    public ConfigOption<T> changeKey(String key) {
        return new ConfigOption<>(key, valueConvert, defaultValue, description, hasDefaultValue);
    }

    public ConfigOption<T> concatHead(String head) {
        return changeKey(head + key);
    }

    public String key() {
        return key;
    }

    public T defaultValue() {
        return defaultValue;
    }

    public String description() {
        return description;
    }

    public boolean hasDefaultValue() {
        return hasDefaultValue;
    }

    public T parseValue(Object value) {
        return valueConvert.apply(value);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[key:");
        sb.append(key);
        if (description != null) {
            sb.append(", description:");
            sb.append(description);
        }
        if (hasDefaultValue) {
            sb.append(", default:");
            sb.append(defaultValue);
        }
        sb.append("]");
        return sb.toString();
    }
}
