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

package org.zicat.tributary.server.component;

import io.prometheus.client.Collector;
import org.zicat.tributary.common.IOUtils;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** AbstractComponent. */
public abstract class AbstractComponent<ID, ELEMENT extends Closeable> extends Collector
        implements Component<ID, ELEMENT> {

    protected final AtomicBoolean close = new AtomicBoolean(false);
    protected final Map<ID, ELEMENT> elements;

    public AbstractComponent(Map<ID, ELEMENT> elements) {
        this.elements = Collections.unmodifiableMap(elements);
    }

    @Override
    public ELEMENT get(ID id) {
        return elements.get(id);
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void close() {
        if (close.compareAndSet(false, true)) {
            elements.forEach((k, v) -> IOUtils.closeQuietly(v));
        }
    }
}
