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

package org.zicat.tributary.channel.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/** CompressionTypeTest. */
public class CompressionTypeTest {

    Random random = new Random(123123);

    @Test
    public void test() throws IOException {
        test(new CompressTypeEntity(CompressionType.NONE, (byte) 1, "none"));
        test(new CompressTypeEntity(CompressionType.ZSTD, (byte) 2, "zstd"));
        test(new CompressTypeEntity(CompressionType.SNAPPY, (byte) 3, "snappy"));
    }

    /**
     * test compression type.
     *
     * @param entity entity
     * @throws IOException IOException
     */
    private void test(CompressTypeEntity entity) throws IOException {
        final CompressionType compressionType = entity.type;
        Assert.assertEquals(CompressionType.getById(entity.expectedId), compressionType);
        Assert.assertEquals(CompressionType.getByName(entity.expectedName), compressionType);
        final ByteBuffer byteBuffer = testArray();
        final byte[] expectedArray = new byte[byteBuffer.remaining()];
        byteBuffer.duplicate().get(expectedArray);
        final ByteBuffer compressBuffer =
                compressionType.compression(
                        byteBuffer, ByteBuffer.allocateDirect(byteBuffer.remaining()));
        // read length
        compressBuffer.getInt();

        final ByteBuffer decompressionBuffer =
                compressionType.decompression(
                        compressBuffer, ByteBuffer.allocateDirect(compressBuffer.remaining()));
        final byte[] finalArray = new byte[decompressionBuffer.remaining()];
        decompressionBuffer.get(finalArray);
        Assert.assertArrayEquals(expectedArray, finalArray);
    }

    /**
     * create test array.
     *
     * @return byte array
     */
    private ByteBuffer testArray() {
        final int length = 4096;
        final int capacity = length + 10;
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(capacity);
        for (int i = 0; i < capacity; i++) {
            byteBuffer.put((byte) random.nextInt(100));
        }
        byteBuffer.flip();
        for (int i = 0; i < capacity - length; i++) {
            byteBuffer.get();
        }
        return byteBuffer;
    }

    /** CompressTypeEntity. */
    private static class CompressTypeEntity {
        final byte expectedId;
        final String expectedName;
        final CompressionType type;

        public CompressTypeEntity(CompressionType type, byte expectedId, String expectedName) {
            this.expectedId = expectedId;
            this.expectedName = expectedName;
            this.type = type;
        }
    }
}
