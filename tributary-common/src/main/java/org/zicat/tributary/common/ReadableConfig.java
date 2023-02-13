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

import java.util.Properties;

/** ReadableConfig. */
public interface ReadableConfig {

    /**
     * get value by config option.
     *
     * @param configOption configOption
     * @param <T> type of value
     * @return value
     */
    <T> T get(ConfigOption<T> configOption);

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
}
