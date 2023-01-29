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

package org.zicat.tributary.demo.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.source.netty.client.LengthDecoderClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/** NettyLengthDecoderTool. */
public class NettyLengthDecoderTool {

    private static final Logger LOG = LoggerFactory.getLogger(NettyLengthDecoderTool.class);

    public static void main(String[] args) throws IOException {
        final Random random = new Random();
        try (LengthDecoderClient client = new LengthDecoderClient(8200)) {
            int length = random.nextInt(100) + 50;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                sb.append(i % 2 == 0 ? "a" : "b");
            }
            byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
            int response = client.append(sb.toString().getBytes(StandardCharsets.UTF_8));
            LOG.info("send length:{}, response length:{}", data.length, response);
        }
    }
}
