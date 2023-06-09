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

package org.zicat.tributary.sink.hdfs.test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** MockFsDataOutputStream. */
public class MockFsDataOutputStream extends FSDataOutputStream {

    private static final Logger logger = LoggerFactory.getLogger(MockFsDataOutputStream.class);
    boolean closeSucceed;

    public MockFsDataOutputStream(FSDataOutputStream wrapMe, boolean closeSucceed) {
        super(wrapMe.getWrappedStream(), null);
        this.closeSucceed = closeSucceed;
    }

    @Override
    public void close() throws IOException {
        logger.info("Close Succeeded - " + closeSucceed);
        if (closeSucceed) {
            logger.info("closing file");
            super.close();
        } else {
            throw new IOException("MockIOException");
        }
    }
}
