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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.zicat.tributary.sink.hdfs.test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.hdfs.HDFSRecordsWriter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/** MockHDFSWriter. */
public class MockHDFSRecordsWriter implements HDFSRecordsWriter {

    private int filesOpened = 0;
    private int filesClosed = 0;
    private int bytesWritten = 0;
    private int eventsWritten = 0;
    private String filePath = null;
    private static final Logger logger = LoggerFactory.getLogger(MockHDFSRecordsWriter.class);
    private final int numberOfRetriesRequired;
    public volatile AtomicInteger currentCloseAttempts = new AtomicInteger(0);

    public MockHDFSRecordsWriter() {
        this.numberOfRetriesRequired = 0;
    }

    public int getFilesOpened() {
        return filesOpened;
    }

    public int getFilesClosed() {
        return filesClosed;
    }

    public int getBytesWritten() {
        return bytesWritten;
    }

    public int getEventsWritten() {
        return eventsWritten;
    }

    public String getOpenedFilePath() {
        return filePath;
    }

    @Override
    public void open(FileSystem fileSystem, Path path) {
        this.filePath = path.getName();
        filesOpened++;
    }

    @Override
    public int append(Records records) throws IOException {
        eventsWritten += records.count();
        int size = 0;
        for (Record record : records) {
            size += record.value().length;
        }
        bytesWritten += size;
        return size;
    }

    public void close() throws IOException {
        filesClosed++;
        int curr = currentCloseAttempts.incrementAndGet();
        logger.info(
                "Attempting to close: '"
                        + currentCloseAttempts
                        + "' of '"
                        + numberOfRetriesRequired
                        + "'");
        if (curr >= numberOfRetriesRequired || numberOfRetriesRequired == 0) {
            logger.info("closing file");
        } else {
            throw new IOException("MockIOException");
        }
    }
}
