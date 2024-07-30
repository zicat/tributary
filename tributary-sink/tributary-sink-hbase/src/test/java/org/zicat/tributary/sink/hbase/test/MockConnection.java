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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/** MockConnection. */
public class MockConnection implements Connection {

    private final Configuration configuration;
    private final AtomicBoolean close = new AtomicBoolean(false);
    private final Map<TableName, MockBufferedMutator> tableNameMap = new HashMap<>();

    public MockConnection(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public MockBufferedMutator getBufferedMutator(TableName tableName) {
        return tableNameMap.computeIfAbsent(
                tableName, k -> new MockBufferedMutator(tableName, configuration));
    }

    @Override
    public MockBufferedMutator getBufferedMutator(BufferedMutatorParams params) {
        return getBufferedMutator(params.getTableName());
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) {
        return null;
    }

    @Override
    public void clearRegionLocationCache() {}

    @Override
    public Admin getAdmin() {
        return null;
    }

    @Override
    public void close() {
        close.set(true);
    }

    @Override
    public boolean isClosed() {
        return close.get();
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
        return null;
    }

    @Override
    public void abort(String why, Throwable e) {}

    @Override
    public boolean isAborted() {
        return false;
    }
}
