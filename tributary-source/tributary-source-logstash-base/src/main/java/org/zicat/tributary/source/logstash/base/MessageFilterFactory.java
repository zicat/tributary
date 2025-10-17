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

package org.zicat.tributary.source.logstash.base;

import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.SpiFactory;

import java.io.Closeable;

/** MessageFilterFactory. */
public interface MessageFilterFactory extends Closeable, SpiFactory {

    /**
     * open.
     *
     * @param config config
     */
    void open(ReadableConfig config) throws Exception;

    /**
     * getMessageFilter.
     *
     * @return MessageFilter
     */
    MessageFilter<Object> getMessageFilter();

    /**
     * find source factory by id.
     *
     * @param identity identity
     * @return SpiFactory
     */
    static MessageFilterFactory findFactory(String identity) {
        return SpiFactory.findFactory(identity, MessageFilterFactory.class);
    }
}
