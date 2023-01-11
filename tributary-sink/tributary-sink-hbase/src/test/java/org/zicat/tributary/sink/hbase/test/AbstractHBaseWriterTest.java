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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.zicat.tributary.sink.hbase.AbstractHBaseWriter;
import org.zicat.tributary.sink.hbase.HTableEntity;

import java.io.IOException;

/** HbaseWriterTest. */
public class AbstractHBaseWriterTest {
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
    public void testBufferFlushMaxMutations() throws IOException {

        final int appendCountFlush = 10;
        final Configuration configuration = new Configuration();
        configuration.setLong("appendCountFlush", appendCountFlush);
        final MockConnection connection = Mockito.spy(new MockConnection(configuration));
        Mockito.when(ConnectionFactory.createConnection(configuration)).thenReturn(connection);

        final HTableEntity tableEntity = new HTableEntity("aa", "bb");

        final AbstractHBaseWriter hbaseWriterAbstract =
                new AbstractHBaseWriter(tableEntity) {
                    @Override
                    public Configuration createConfiguration(HTableEntity hTableEntity) {
                        return configuration;
                    }

                    @Override
                    public BufferedMutatorParams createBufferedMutatorParams(
                            HTableEntity hTableEntity) {
                        return new BufferedMutatorParams(hTableEntity.tableName());
                    }
                };
        hbaseWriterAbstract.open();

        final MockBufferedMutator mutator =
                (MockBufferedMutator) connection.getBufferedMutator(tableEntity.tableName());

        for (int i = 0; i < appendCountFlush + 1; i++) {
            hbaseWriterAbstract.appendData(null);
        }
        Assert.assertEquals(1, mutator.flushCount.get());
        for (int i = 0; i < appendCountFlush + 1; i++) {
            hbaseWriterAbstract.appendData(null);
        }
        Assert.assertEquals(2, mutator.flushCount.get());

        hbaseWriterAbstract.appendData(null);
        hbaseWriterAbstract.close();
        Assert.assertEquals(3, mutator.flushCount.get());
        Assert.assertTrue(connection.isClosed());
    }
}
