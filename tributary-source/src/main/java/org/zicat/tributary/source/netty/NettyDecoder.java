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

import org.zicat.tributary.common.SafeFactory;
import org.zicat.tributary.source.netty.ack.AckHandler;
import org.zicat.tributary.source.netty.ack.LengthAckHandler;
import org.zicat.tributary.source.netty.ack.MuteAckHandler;

/** NettyDecoder. */
public enum NettyDecoder {
    lineDecoder(LineDecoder::new, MuteAckHandler::new),
    lengthDecoder(LengthDecoder::new, LengthAckHandler::new);

    private final SourceDecoderFactory sourceDecoderFactory;
    private final AckHandlerFactory ackHandlerFactory;

    NettyDecoder(SourceDecoderFactory sourceDecoderFactory, AckHandlerFactory ackHandlerFactory) {
        this.sourceDecoderFactory = sourceDecoderFactory;
        this.ackHandlerFactory = ackHandlerFactory;
    }

    /**
     * create ack handler.
     *
     * @return AckHandler
     */
    public AckHandler createAckHandler() {
        return ackHandlerFactory.create();
    }

    /**
     * create source decoder.
     *
     * @return source decoder
     */
    public SourceDecoder createSourceDecoder() {
        return sourceDecoderFactory.create();
    }

    /** SourceDecoderFactory to create SourceDecoder. */
    interface SourceDecoderFactory extends SafeFactory<SourceDecoder> {}

    /** AckHandlerFactory to create AckHandler. */
    interface AckHandlerFactory extends SafeFactory<AckHandler> {}
}
