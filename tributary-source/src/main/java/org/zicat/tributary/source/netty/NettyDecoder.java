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

package org.zicat.tributary.source.netty;

/** NettyDecoder. */
public enum NettyDecoder {
    lineDecoder(false, LineDecoder::new),
    lengthDecoder(true, LengthDecoder::new);

    private final boolean ack;
    private final SourceDecoderFactory sourceDecoderFactory;

    NettyDecoder(boolean ack, SourceDecoderFactory sourceDecoderFactory) {
        this.ack = ack;
        this.sourceDecoderFactory = sourceDecoderFactory;
    }

    /**
     * is ack.
     *
     * @return boolean
     */
    public boolean isAck() {
        return ack;
    }

    /**
     * create source decoder.
     *
     * @return source decoder
     */
    public SourceDecoder createSourceDecoder() {
        return sourceDecoderFactory.createSourceDecoder();
    }

    interface SourceDecoderFactory {
        SourceDecoder createSourceDecoder();
    }
}
