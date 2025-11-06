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

import static org.zicat.tributary.sink.hbase.HBaseFunctionFactory.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.hbase.HBaseFunction;

import java.util.*;

/** HBaseFunctionTest. */
public class HBaseFunctionTest {

    private static final String topic = "kt1";
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
    public void test() throws Exception {
        final Map<String, byte[]> recordHeader1 = new HashMap<>();
        recordHeader1.put("rhk1", "rhv1".getBytes());
        final Record record1 = new DefaultRecord(recordHeader1, "rk1".getBytes(), "rv1".getBytes());
        final Map<String, byte[]> recordHeader2 = new HashMap<>();
        recordHeader2.put("rhk2", "rhv2".getBytes());
        final Record record2 = new DefaultRecord(recordHeader2, "rk2".getBytes(), "rv2".getBytes());
        final Map<String, byte[]> recordsHeader = new HashMap<>();
        recordsHeader.put("rshk1", "rshv1".getBytes());
        final List<Records> recordsList =
                Collections.singletonList(
                        new DefaultRecords(topic, recordsHeader, Arrays.asList(record1, record2)));

        final Configuration configuration = new Configuration();
        final MockConnection connection = Mockito.spy(new MockConnection(configuration));
        Mockito.when(ConnectionFactory.createConnection(configuration)).thenReturn(connection);

        final ContextBuilder builder =
                new ContextBuilder().id("f1").groupId("g1").partitionId(0).topic("t1");
        builder.addCustomProperty(OPTION_HBASE_SITE_XML_PATH, "test-hbase-site.xml");
        builder.addCustomProperty(OPTION_HBASE_FAMILY, "f1");
        builder.addCustomProperty(OPTION_HBASE_COLUMN_VALUE_NAME, "v1");
        builder.addCustomProperty(OPTION_HBASE_COLUMN_HEAD_PREFIX, "h_");
        builder.addCustomProperty(OPTION_HBASE_TABLE_NAME, "tl1");
        builder.addCustomProperty(OPTION_HBASE_COLUMN_TOPIC_NAME, "tp1");
        try (Function function =
                new HBaseFunction() {
                    @Override
                    protected Configuration createConfiguration() {
                        final Configuration oldConf = super.createConfiguration();
                        Assert.assertEquals("10", oldConf.get("aaa"));
                        return configuration;
                    }
                }) {
            function.open(builder.build());
            function.process(Offset.ZERO, recordsList.iterator());

            final MockBufferedMutator bufferedMutator =
                    connection.getBufferedMutator(TableName.valueOf("tl1"));
            final List<Mutation> mutateList = bufferedMutator.mutateList;
            final byte[] family = Bytes.toBytes("f1");
            Assert.assertEquals(2, mutateList.size());
            final Mutation m1 = mutateList.get(0);
            Assert.assertEquals("rk1", Bytes.toString(m1.getRow()));
            Assert.assertEquals("rv1", toString(m1.get(family, Bytes.toBytes("v1")).get(0)));
            Assert.assertEquals(topic, toString(m1.get(family, Bytes.toBytes("tp1")).get(0)));
            Assert.assertEquals("rhv1", toString(m1.get(family, Bytes.toBytes("h_rhk1")).get(0)));
            Assert.assertEquals("rshv1", toString(m1.get(family, Bytes.toBytes("h_rshk1")).get(0)));
            Assert.assertNotNull(toString(m1.get(family, Bytes.toBytes("h__sent_ts")).get(0)));

            final Mutation m2 = mutateList.get(1);
            Assert.assertEquals("rk2", Bytes.toString(m2.getRow()));
            Assert.assertEquals("rv2", toString(m2.get(family, Bytes.toBytes("v1")).get(0)));
            Assert.assertEquals(topic, toString(m2.get(family, Bytes.toBytes("tp1")).get(0)));
            Assert.assertEquals("rhv2", toString(m2.get(family, Bytes.toBytes("h_rhk2")).get(0)));
            Assert.assertEquals("rshv1", toString(m2.get(family, Bytes.toBytes("h_rshk1")).get(0)));
            Assert.assertNotNull(toString(m2.get(family, Bytes.toBytes("h__sent_ts")).get(0)));

            final List<Mutation> flushList = bufferedMutator.flushedList;
            Assert.assertTrue(flushList.isEmpty());
            function.snapshot();
            Assert.assertEquals(2, flushList.size());
        }
    }

    private static String toString(Cell cell) {
        return Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }
}
