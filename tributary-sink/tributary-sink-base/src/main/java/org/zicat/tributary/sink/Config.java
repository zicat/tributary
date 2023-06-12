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
import org.zicat.tributary.common.DefaultReadableConfig;

import java.util.Map;
import java.util.Properties;

/** Config. */
public class Config extends DefaultReadableConfig {

    protected static final Logger LOG = LoggerFactory.getLogger(Config.class);

    protected Config(Map<String, Object> customConfig) {
        putAll(customConfig);
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
        for (Map.Entry<String, Object> entry : entrySet()) {
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
}
