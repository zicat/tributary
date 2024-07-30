package org.zicat.tributary.sink.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** MockBufferedMutator. @ThreadSafe */
public class MockBufferedMutator implements BufferedMutator {
    private final TableName tableName;
    private final Configuration configuration;
    public final List<Mutation> mutateList = Collections.synchronizedList(new ArrayList<>());
    public final AtomicInteger flushCount = new AtomicInteger();
    public final AtomicInteger flushSize = new AtomicInteger();
    public final List<Mutation> flushedList = Collections.synchronizedList(new ArrayList<>());
    private final long appendCountFlush;

    public MockBufferedMutator(TableName tableName, Configuration configuration) {
        this.tableName = tableName;
        this.configuration = configuration;
        this.appendCountFlush = configuration.getLong("appendCountFlush", Integer.MAX_VALUE);
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public synchronized void mutate(Mutation mutation) {
        if (mutateList.size() == appendCountFlush) {
            flush();
        }
        mutateList.add(mutation);
    }

    @Override
    public synchronized void mutate(List<? extends Mutation> mutations) {
        mutations.forEach(this::mutate);
    }

    @Override
    public synchronized void close() {
        if (!mutateList.isEmpty()) {
            flush();
        }
    }

    @Override
    public synchronized void flush() {
        flushCount.incrementAndGet();
        flushSize.addAndGet(mutateList.size());
        flushedList.addAll(mutateList);
        mutateList.clear();
    }

    @Override
    public long getWriteBufferSize() {
        return 0;
    }

    @Override
    public void setRpcTimeout(int timeout) {}

    @Override
    public void setOperationTimeout(int timeout) {}
}
