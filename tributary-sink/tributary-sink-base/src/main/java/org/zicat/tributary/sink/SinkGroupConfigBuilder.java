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

import org.zicat.tributary.sink.handler.factory.SimplePartitionHandlerFactory;

/** SinkGroupConfigBuilder. */
public class SinkGroupConfigBuilder extends CustomConfigBuilder {

    private String handlerIdentify = SimplePartitionHandlerFactory.IDENTIFY;
    private String processFunctionIdentify;

    /** @return builder */
    public static SinkGroupConfigBuilder newBuilder() {
        return new SinkGroupConfigBuilder();
    }

    /**
     * set processFunctionIdentify.
     *
     * @param processFunctionIdentify processFunctionIdentify
     * @return SinkGroupConfigBuilder
     */
    public SinkGroupConfigBuilder processFunctionIdentify(String processFunctionIdentify) {
        if (processFunctionIdentify == null) {
            throw new IllegalArgumentException("processFunctionIdentify is null");
        }
        this.processFunctionIdentify = processFunctionIdentify;
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
        if (processFunctionIdentify == null) {
            throw new IllegalStateException("process function identify must not null");
        }
        return new SinkGroupConfig(handlerIdentify, processFunctionIdentify, customConfig);
    }
}
