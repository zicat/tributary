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

import org.zicat.tributary.common.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/** Factory. */
public interface Factory<T> {

    /**
     * create element.
     *
     * @return element.
     * @throws IOException IOException
     */
    T create() throws IOException;

    /**
     * destroy element.
     *
     * @param t t
     */
    default void destroy(T t) {
        if (t == null) {
            return;
        }
        if (t instanceof Closeable) {
            IOUtils.closeQuietly((Closeable) t);
            return;
        }
        throw new IllegalStateException("element t is not closeable " + t.getClass());
    }
}
