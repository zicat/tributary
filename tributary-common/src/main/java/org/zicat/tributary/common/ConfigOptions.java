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

package org.zicat.tributary.common;

import java.util.function.Function;

/** ConfigOption. */
public class ConfigOptions {

    public static ConfigOptionTypeBuilder key(String key) {
        return new ConfigOptionTypeBuilder(key);
    }

    /** ConfigOptionTypeBuilder. */
    @SuppressWarnings("unchecked")
    public static class ConfigOptionTypeBuilder {

        private final String key;

        public ConfigOptionTypeBuilder(String key) {
            this.key = key;
        }

        public final Builder<String> stringType() {
            return new Builder<>(key, Object::toString);
        }

        public final Builder<Integer> integerType() {
            return new Builder<>(key, s -> Integer.parseInt(s.toString()));
        }

        public final Builder<Long> longType() {
            return new Builder<>(key, s -> Long.parseLong(s.toString()));
        }

        public final Builder<Boolean> booleanType() {
            return new Builder<>(key, s -> Boolean.parseBoolean(s.toString()));
        }

        public final <T> Builder<T> objectType() {
            return new Builder<>(key, o -> (T) o);
        }
    }

    /**
     * Builder.
     *
     * @param <T>
     */
    public static class Builder<T> {

        private final String key;
        private final Function<Object, T> valueConvert;
        private String description;

        public Builder(String key, Function<Object, T> valueConvert) {
            this.key = key;
            this.valueConvert = valueConvert;
        }

        public Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        /**
         * set value.
         *
         * @param v v
         * @return ConfigOption
         */
        public ConfigOption<T> defaultValue(T v) {
            return new ConfigOption<>(key, valueConvert, v, description, true);
        }

        /**
         * set no default value.
         *
         * @return ConfigOption
         */
        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(key, valueConvert, null, description, false);
        }
    }
}
