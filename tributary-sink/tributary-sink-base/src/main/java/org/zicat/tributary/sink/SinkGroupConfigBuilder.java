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

import org.zicat.tributary.sink.handler.factory.DirectPartitionHandlerFactory;

/** SinkGroupConfigBuilder. */
public class SinkGroupConfigBuilder extends CustomConfigBuilder {

    private String handlerIdentify = DirectPartitionHandlerFactory.IDENTIFY;
    private String functionIdentify;

    /** @return builder */
    public static SinkGroupConfigBuilder newBuilder() {
        return new SinkGroupConfigBuilder();
    }

    /**
     * set functionIdentify.
     *
     * @param functionIdentify functionIdentify
     * @return SinkGroupConfigBuilder
     */
    public SinkGroupConfigBuilder functionIdentify(String functionIdentify) {
        if (functionIdentify == null) {
            throw new IllegalArgumentException("functionIdentify is null");
        }
        this.functionIdentify = functionIdentify;
        return this;
    }

    /**
     * set handlerIdentify.
     *
     * @param handlerIdentify handlerIdentify
     * @return SinkGroupConfigBuilder
     */
    public SinkGroupConfigBuilder handlerIdentify(String handlerIdentify) {
        if (handlerIdentify == null) {
            throw new IllegalArgumentException("handlerIdentify is null");
        }
        this.handlerIdentify = handlerIdentify;
        return this;
    }

    /**
     * build SinkGroupConfig.
     *
     * @return SinkGroupConfig
     */
    public SinkGroupConfig build() {
        if (functionIdentify == null) {
            throw new IllegalStateException("function identify must not null");
        }
        return new SinkGroupConfig(handlerIdentify, functionIdentify, customConfig);
    }
}
