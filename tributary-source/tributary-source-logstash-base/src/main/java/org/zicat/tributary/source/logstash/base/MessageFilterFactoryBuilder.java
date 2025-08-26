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

import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;

/** MessageFilterFactoryManager. */
public class MessageFilterFactoryBuilder {

    public static final ConfigOption<String> OPTION_MESSAGE_FILTER_FACTORY_ID =
            ConfigOptions.key("message-filter-factory.identity")
                    .stringType()
                    .defaultValue(DefaultMessageFilterFactory.IDENTITY);

    private ReadableConfig config;

    /**
     * newBuilder.
     *
     * @return MessageFilterFactoryBuilder
     */
    public static MessageFilterFactoryBuilder newBuilder() {
        return new MessageFilterFactoryBuilder();
    }

    public MessageFilterFactoryBuilder config(ReadableConfig config) {
        this.config = config;
        return this;
    }

    /**
     * build MessageFilterFactory.
     *
     * @return MessageFilterFactory
     */
    public MessageFilterFactory buildAndOpen() throws Exception {
        final String id = config.get(OPTION_MESSAGE_FILTER_FACTORY_ID);
        final MessageFilterFactory factory = MessageFilterFactory.findFactory(id);
        factory.open(config);
        return factory;
    }
}
