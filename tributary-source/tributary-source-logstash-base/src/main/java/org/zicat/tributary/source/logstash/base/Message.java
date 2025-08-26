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

import io.netty.buffer.ByteBuf;

import java.util.Map;

/** Message. */
public class Message<PAYLOAD> implements Comparable<Message<PAYLOAD>> {

    private final int sequence;
    private final Map<String, Object> data;
    private final PAYLOAD payload;

    /**
     * Create a message using a map of key, value pairs.
     *
     * @param sequence sequence number of the message
     * @param data key/value pairs representing the message
     */
    public Message(PAYLOAD batch, int sequence, Map<String, Object> data) {
        this.sequence = sequence;
        this.payload = batch;
        this.data = data;
    }

    /**
     * Create a message using a map of key, value pairs.
     *
     * @param sequence sequence number of the message
     * @param data key/value pairs representing the message
     */
    public Message(int sequence, Map<String, Object> data) {
        this(null, sequence, data);
    }

    /**
     * Returns the sequence number of this message.
     *
     * @return sequence number of the message
     */
    public int getSequence() {
        return sequence;
    }

    /**
     * Returns a list of key/value pairs representing the contents of the message. Note that this
     * method is lazy if the Message was created using a {@link ByteBuf}.
     *
     * @return {@link Map} Map of key/value pairs
     */
    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public int compareTo(Message o) {
        return Integer.compare(getSequence(), o.getSequence());
    }

    public PAYLOAD getPayload() {
        return payload;
    }
}
