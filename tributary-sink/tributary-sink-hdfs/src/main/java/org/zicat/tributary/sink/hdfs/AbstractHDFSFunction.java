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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.Strings;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.authentication.PrivilegedExecutor;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.*;

import static org.zicat.tributary.sink.authentication.TributaryAuthenticationUtil.getAuthenticator;

/** AbstractHDFSFunction. */
public abstract class AbstractHDFSFunction<P> extends AbstractFunction {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractHDFSFunction.class);

    public static final String DIRECTORY_DELIMITER = FileSystems.getDefault().getSeparator();

    public static final ConfigOption<String> OPTION_SINK_PATH =
            ConfigOptions.key("sink.path")
                    .stringType()
                    .description("set sink base path")
                    .noDefaultValue();

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
            ConfigOptions.key("max.retries")
                    .integerType()
                    .description("max retries times if operation fail")
                    .defaultValue(3);

    public static final ConfigOption<String> OPTION_OUTPUT_COMPRESSION_CODEC =
            ConfigOptions.key("output.compression.codec")
                    .stringType()
                    .description("set output compression codec, default snappy")
                    .defaultValue("snappy");

    protected PrivilegedExecutor privilegedExecutor;
    protected HDFSWriterFactory hdfsWriterFactory;
    protected String basePath;
    protected long rollSize;
    protected Integer maxRetry;
    protected Map<String, BucketWriter<P>> sfWriters = new HashMap<>();
    protected String prefixFileName;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        this.hdfsWriterFactory =
                new ParquetHDFSWriterFactory(context.get(OPTION_OUTPUT_COMPRESSION_CODEC));
        final String basePath = context.get(OPTION_SINK_PATH).trim();
        this.basePath = Strings.removeLastIfMatch(basePath, DIRECTORY_DELIMITER);
        this.privilegedExecutor =
                getAuthenticator(context.get(OPTION_PRINCIPLE), context.get(OPTION_KEYTAB));
        this.rollSize = context.get(OPTION_ROLL_SIZE);
        this.maxRetry = context.get(OPTION_MAX_RETRIES);
        this.prefixFileName = prefixFileNameInBucket();
    }

    /**
     * append data.
     *
     * @param bucket bucket
     * @param records records
     * @throws IOException IOException
     */
    public void appendData(String bucket, Records records) throws IOException {
        BucketWriter<P> bucketWriter = sfWriters.get(bucket);
        if (bucketWriter == null) {
            final String bucketPath = basePath + DIRECTORY_DELIMITER + bucket;
            bucketWriter = initializeBucketWriter(bucketPath, prefixFileName);
            sfWriters.put(bucket, bucketWriter);
            LOG.info("create hdfs file {}", bucket);
        }
        bucketWriter.append(records);
    }

    /**
     * remove bucket.
     *
     * @param bucket bucket.
     * @throws IOException IOException
     */
    public P closeBucket(String bucket) throws Exception {
        final BucketWriter<P> bucketWriter = sfWriters.remove(bucket);
        if (bucketWriter != null) {
            bucketWriter.close();
        }
        return bucketWriter == null ? null : bucketWriter.payload();
    }

    /**
     * close all buckets.
     *
     * @return list payload
     * @throws Exception Exception
     */
    public List<P> closeAllBuckets() throws Exception {
        final List<P> result = new ArrayList<>(sfWriters.size());
        for (Map.Entry<String, BucketWriter<P>> entry : sfWriters.entrySet()) {
            result.add(closeBucket(entry.getKey()));
        }
        return result;
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
                hdfsWriterFactory,
                privilegedExecutor,
                rollSize,
                maxRetry,
                null);
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
