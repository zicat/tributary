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

package org.zicat.tributary.sink.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.hbase.AbstractHBaseFunction;
import org.zicat.tributary.sink.hbase.DiscardHBaseWriter;
import org.zicat.tributary.sink.hbase.HTableEntity;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.zicat.tributary.sink.function.AbstractFunction.KEY_FLUSH_MILL;

/** AbstractHBaseFunctionTest. */
public class AbstractHBaseFunctionTest {
    MockedStatic<ConnectionFactory> factoryMockedStatic;

    @Before
    public void setup() {
        factoryMockedStatic = Mockito.mockStatic(ConnectionFactory.class);
    }

    @After
    public void close() {
        factoryMockedStatic.close();
    }

    @Test
    public void testDiscardHBaseWriter() throws Exception {
        final Configuration configuration = new Configuration();
        final MockConnection connection = Mockito.spy(new MockConnection(configuration));
        Mockito.when(ConnectionFactory.createConnection(configuration)).thenReturn(connection);
        final HTableEntity tableIdentify = new HTableEntity("test", "counter");
        final AbstractHBaseFunction function =
                new AbstractHBaseFunction() {
                    @Override
                    public Configuration createHBaseConf(HTableEntity tableEntity) {
                        throw new RuntimeException("test");
                    }

                    @Override
                    public BufferedMutatorParams createBufferedMutatorParams(HTableEntity entity) {
                        return new BufferedMutatorParams(entity.tableName());
                    }

                    @Override
                    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator)
                            throws Exception {
                        while (iterator.hasNext()) {
                            byte[] data = iterator.next();
                            sendDataToHbase(tableIdentify, new Put(data));
                        }
                    }
                };
        final ContextBuilder contextBuilder =
                ContextBuilder.newBuilder()
                        .id("id")
                        .groupId("g1")
                        .partitionId(0)
                        .topic("t1")
                        .startRecordsOffset(RecordsOffset.startRecordOffset());
        function.open(contextBuilder.build());
        final List<byte[]> testData =
                Arrays.asList(
                        "1".getBytes(StandardCharsets.UTF_8),
                        "2".getBytes(StandardCharsets.UTF_8),
                        "3".getBytes(StandardCharsets.UTF_8));
        function.process(new RecordsOffset(1, 1), testData.listIterator());
        Assert.assertTrue(function.getHBaseWriter(tableIdentify) instanceof DiscardHBaseWriter);
        function.close();
    }

    @Test
    public void testAppendData() throws Exception {
        final Configuration configuration = new Configuration();
        final MockConnection connection = Mockito.spy(new MockConnection(configuration));
        Mockito.when(ConnectionFactory.createConnection(configuration)).thenReturn(connection);
        final HTableEntity tableIdentify = new HTableEntity("test", "counter");
        final AbstractHBaseFunction function =
                new AbstractHBaseFunction() {
                    @Override
                    public Configuration createHBaseConf(HTableEntity tableEntity) {
                        return configuration;
                    }

                    @Override
                    public BufferedMutatorParams createBufferedMutatorParams(HTableEntity entity) {
                        return new BufferedMutatorParams(entity.tableName());
                    }

                    @Override
                    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator)
                            throws Exception {
                        while (iterator.hasNext()) {
                            byte[] data = iterator.next();
                            sendDataToHbase(tableIdentify, new Put(data));
                        }
                    }
                };
        final ContextBuilder contextBuilder =
                ContextBuilder.newBuilder()
                        .id("id")
                        .groupId("g1")
                        .partitionId(0)
                        .topic("t1")
                        .startRecordsOffset(RecordsOffset.startRecordOffset());
        function.open(contextBuilder.build());
        final List<byte[]> testData =
                Arrays.asList(
                        "1".getBytes(StandardCharsets.UTF_8),
                        "2".getBytes(StandardCharsets.UTF_8),
                        "3".getBytes(StandardCharsets.UTF_8));
        function.process(new RecordsOffset(1, 1), testData.listIterator());

        final MockBufferedMutator mutator =
                (MockBufferedMutator) connection.getBufferedMutator(tableIdentify.tableName());
        Assert.assertEquals(3, mutator.mutateList.size());
        function.process(new RecordsOffset(1, 1), testData.listIterator());
        Assert.assertEquals(6, mutator.mutateList.size());
        function.close();
    }

    @Test
    public void testClose() throws Exception {
        final Configuration configuration = new Configuration();
        final MockConnection connection = Mockito.spy(new MockConnection(configuration));
        Mockito.when(ConnectionFactory.createConnection(configuration)).thenReturn(connection);
        final HTableEntity hTableEntity = new HTableEntity("test", "counter");
        final AbstractHBaseFunction function =
                new AbstractHBaseFunction() {
                    @Override
                    public Configuration createHBaseConf(HTableEntity entity) {
                        return configuration;
                    }

                    @Override
                    public BufferedMutatorParams createBufferedMutatorParams(HTableEntity entity) {
                        return new BufferedMutatorParams(entity.tableName());
                    }

                    @Override
                    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator)
                            throws Exception {
                        while (iterator.hasNext()) {
                            byte[] data = iterator.next();
                            sendDataToHbase(hTableEntity, new Put(data));
                        }
                    }
                };
        final ContextBuilder contextBuilder =
                ContextBuilder.newBuilder()
                        .id("id")
                        .groupId("g1")
                        .partitionId(0)
                        .topic("t1")
                        .startRecordsOffset(RecordsOffset.startRecordOffset());
        function.open(contextBuilder.build());
        final List<byte[]> testData =
                Arrays.asList(
                        "1".getBytes(StandardCharsets.UTF_8),
                        "2".getBytes(StandardCharsets.UTF_8),
                        "3".getBytes(StandardCharsets.UTF_8),
                        "4".getBytes(StandardCharsets.UTF_8));
        function.process(new RecordsOffset(1, 1), testData.listIterator());

        final MockBufferedMutator mutator =
                (MockBufferedMutator) connection.getBufferedMutator(hTableEntity.tableName());
        Assert.assertEquals(0, mutator.flushCount.get());
        Assert.assertEquals(0, mutator.flushSize.get());
        function.process(new RecordsOffset(1, 1), testData.listIterator());
        function.close();
        Assert.assertEquals(1, mutator.flushCount.get());
        Assert.assertEquals(8, mutator.flushSize.get());
        Assert.assertEquals(0, function.hBaseConnectionCount());
    }

    @Test
    public void testFlush() throws Exception {
        final Configuration configuration = new Configuration();
        final MockConnection connection = Mockito.spy(new MockConnection(configuration));
        Mockito.when(ConnectionFactory.createConnection(configuration)).thenReturn(connection);
        final AbstractHBaseFunction function =
                new AbstractHBaseFunction() {
                    @Override
                    public Configuration createHBaseConf(HTableEntity hTableEntity) {
                        return configuration;
                    }

                    @Override
                    public BufferedMutatorParams createBufferedMutatorParams(
                            HTableEntity hTableEntity) {
                        return new BufferedMutatorParams(hTableEntity.tableName());
                    }

                    @Override
                    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) {}
                };
        final ContextBuilder contextBuilder =
                ContextBuilder.newBuilder()
                        .id("id")
                        .groupId("g1")
                        .partitionId(0)
                        .topic("t1")
                        .startRecordsOffset(new RecordsOffset(1, 1));
        // always flush
        contextBuilder.addCustomProperty(KEY_FLUSH_MILL, 0);
        function.open(contextBuilder.build());
        final RecordsOffset flushRecordOffset = new RecordsOffset(1, 5);
        Assert.assertTrue(function.flush(flushRecordOffset));
        Assert.assertEquals(function.committableOffset(), flushRecordOffset);
    }
}
