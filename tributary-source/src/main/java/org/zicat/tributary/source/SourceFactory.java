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

package org.zicat.tributary.source;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.TributaryRuntimeException;

import java.util.ServiceLoader;

/** SourceFactory. */
public interface SourceFactory {

    /**
     * get factory id.
     *
     * @return string
     */
    String identity();

    /**
     * create server.
     *
     * @param channel channel
     * @param config config
     * @return TributaryServer
     */
    Source createTributaryServer(Channel channel, ReadableConfig config);

    /**
     * find tributary server factory by id.
     *
     * @param identity identity
     * @return TributaryServerFactory
     */
    static SourceFactory findTributaryServerFactory(String identity) {
        final ServiceLoader<SourceFactory> loader = ServiceLoader.load(SourceFactory.class);
        for (SourceFactory sourceFactory : loader) {
            if (identity.equals(sourceFactory.identity())) {
                return sourceFactory;
            }
        }
        throw new TributaryRuntimeException("identity not found," + identity);
    }
}
