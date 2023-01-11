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

package org.zicat.tributary.sink.hbase;

import org.apache.hadoop.hbase.TableName;

import java.util.Objects;

/** HTableEntity. */
public class HTableEntity {
    private final String table;
    private final String cluster;
    private final String tableIdentity;

    public HTableEntity(String cluster, String table) {
        this.cluster = cluster;
        this.table = table;
        this.tableIdentity = cluster + "." + table;
    }

    /**
     * get table.
     *
     * @return table.
     */
    public String table() {
        return table;
    }

    /**
     * get cluster.
     *
     * @return cluster.
     */
    public String cluster() {
        return cluster;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HTableEntity that = (HTableEntity) o;
        return tableIdentity.equals(that.tableIdentity);
    }

    /**
     * get table name.
     *
     * @return TableName
     */
    public final TableName tableName() {
        return TableName.valueOf(table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableIdentity);
    }

    @Override
    public String toString() {
        return tableIdentity;
    }
}
