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

package org.zicat.tributary.sink.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.sink.authentication.PrivilegedExecutor;
import org.zicat.tributary.sink.authentication.TributaryAuthenticationUtil;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** AbstractHDFSFunction. */
public abstract class AbstractHDFSFunction<P> extends AbstractFunction {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractHDFSFunction.class);

    public static final String DIRECTORY_DELIMITER = System.getProperty("file.separator");
    public static final String BASE_SINK_PATH = "sinkPath";

    public static final ConfigOption<String> OPTION_KEYTAB =
            ConfigOptions.key("keytab")
                    .stringType()
                    .description("kerberos keytab")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_PRINCIPLE =
            ConfigOptions.key("principle")
                    .stringType()
                    .description("kerberos principle")
                    .defaultValue(null);

    public static final ConfigOption<Long> OPTION_ROLL_SIZE =
            ConfigOptions.key("roll.size")
                    .longType()
                    .description("roll new file if file size over this param")
                    .defaultValue(1024 * 1024 * 256L);

    public static final ConfigOption<Integer> OPTION_MAX_RETRIES =
            ConfigOptions.key("maxRetries")
                    .integerType()
                    .description("max retries times if operation fail")
                    .defaultValue(3);

    protected PrivilegedExecutor privilegedExecutor;
    protected String basePath;
    protected long rollSize;
    protected Integer maxRetry;
    protected Map<String, BucketWriter<P>> sfWriters = new HashMap<>();
    protected SnappyCodec snappyCodec = new SnappyCodec();
    protected String prefixFileName;

    @Override
    public void open(Context context) {

        super.open(context);
        this.snappyCodec.setConf(new Configuration());
        final String basePath = context.get(BASE_SINK_PATH).toString().trim();
        this.basePath =
                basePath.endsWith(DIRECTORY_DELIMITER)
                        ? basePath.substring(0, basePath.length() - 1)
                        : basePath;
        this.privilegedExecutor =
                TributaryAuthenticationUtil.getAuthenticator(
                        context.get(OPTION_PRINCIPLE), context.get(OPTION_KEYTAB));
        this.rollSize = context.get(OPTION_ROLL_SIZE);
        this.maxRetry = context.get(OPTION_MAX_RETRIES);
        this.prefixFileName = prefixFileNameInBucket();
    }

    /**
     * append data.
     *
     * @param bucket bucket
     * @param bs bs
     * @param offset offset
     * @param length length
     * @throws IOException IOException
     */
    public void appendData(String bucket, byte[] bs, int offset, int length) throws IOException {

        BucketWriter<P> bucketWriter = sfWriters.get(bucket);
        if (bucketWriter == null) {
            final String bucketPath = basePath + DIRECTORY_DELIMITER + bucket;
            bucketWriter = initializeBucketWriter(bucketPath, prefixFileName);
            sfWriters.put(bucket, bucketWriter);
        }
        bucketWriter.append(bs, offset, length);
    }

    /**
     * remove bucket.
     *
     * @param bucket bucket.
     * @throws IOException IOException
     */
    public BucketWriter<P> closeBucket(String bucket) throws Exception {
        final BucketWriter<P> bucketWriter = sfWriters.remove(bucket);
        if (bucketWriter != null) {
            bucketWriter.close();
        }
        return bucketWriter;
    }

    /**
     * create prefix file name.
     *
     * @return prefix file name
     */
    protected String prefixFileNameInBucket() {
        return UUID.randomUUID().toString().replace("-", "_") + "_" + context.id();
    }

    /**
     * init bucket writer.
     *
     * @param bucketPath bucketPath
     * @param realName realName
     * @return BucketWriter
     */
    protected BucketWriter<P> initializeBucketWriter(String bucketPath, String realName) {
        return new BucketWriter<>(
                bucketPath,
                realName,
                snappyCodec,
                new HDFSCompressedDataStream(),
                privilegedExecutor,
                rollSize,
                maxRetry,
                null,
                clock);
    }

    /**
     * flush file offset with writers.
     *
     * @param recordsOffset recordsOffset
     */
    public final void flush(RecordsOffset recordsOffset) {
        final OnFlushCallback flushFunction =
                () -> {
                    for (Map.Entry<String, BucketWriter<P>> entry : sfWriters.entrySet()) {
                        final BucketWriter<P> writer = entry.getValue();
                        writer.flush();
                    }
                    return true;
                };
        flush(recordsOffset, flushFunction);
    }

    @Override
    public void close() {
        for (Map.Entry<String, BucketWriter<P>> entry : sfWriters.entrySet()) {
            final String bucketPath = entry.getKey();
            final BucketWriter<P> writer = entry.getValue();
            LOG.info("Closing {}", bucketPath);
            try {
                writer.close();
            } catch (Exception ex) {
                LOG.warn("closing " + bucketPath + ". " + "Exception follows.", ex);
            }
        }
        sfWriters.clear();
    }
}
