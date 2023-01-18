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

import org.zicat.tributary.sink.handler.DirectPartitionHandlerFactory;

/** SinkGroupConfigBuilder. */
public class SinkGroupConfigBuilder extends CustomConfigBuilder {

    private String handlerIdentity = DirectPartitionHandlerFactory.IDENTITY;
    private String functionIdentity;

    /** @return builder */
    public static SinkGroupConfigBuilder newBuilder() {
        return new SinkGroupConfigBuilder();
    }

    /**
     * set functionIdentity.
     *
     * @param functionIdentity functionIdentity
     * @return SinkGroupConfigBuilder
     */
    public SinkGroupConfigBuilder functionIdentity(String functionIdentity) {
        if (functionIdentity == null) {
            throw new IllegalArgumentException("functionIdentity is null");
        }
        this.functionIdentity = functionIdentity;
        return this;
    }

    /**
     * set handlerIdentity.
     *
     * @param handlerIdentity handlerIdentity
     * @return SinkGroupConfigBuilder
     */
    public SinkGroupConfigBuilder handlerIdentity(String handlerIdentity) {
        if (handlerIdentity == null) {
            throw new IllegalArgumentException("handlerIdentity is null");
        }
        this.handlerIdentity = handlerIdentity;
        return this;
    }

    /**
     * build SinkGroupConfig.
     *
     * @return SinkGroupConfig
     */
    public SinkGroupConfig build() {
        if (functionIdentity == null) {
            throw new IllegalStateException("function identity must not null");
        }
        return new SinkGroupConfig(handlerIdentity, functionIdentity, customConfig);
    }
}
