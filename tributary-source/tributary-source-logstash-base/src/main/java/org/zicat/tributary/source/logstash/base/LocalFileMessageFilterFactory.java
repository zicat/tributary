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

package org.zicat.tributary.source.logstash.base;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.util.ResourceUtils;
import org.zicat.tributary.source.logstash.base.utils.CompileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** LocalFileMessageFilterFactory. */
public class LocalFileMessageFilterFactory implements MessageFilterFactory {

    public static final String IDENTITY = "localFileMessageFilter";
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileMessageFilterFactory.class);
    public static final ConfigOption<String> OPTION_LOCAL_FILE_PATH =
            ConfigOptions.key("local-file-message-filter.path").stringType().noDefaultValue();
    protected File file;
    protected volatile MessageFilter<Object> messageFilter;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected FileAlterationMonitor monitor;

    @Override
    public void open(ReadableConfig config) throws Exception {
        this.file = new File(ResourceUtils.getResourcePath(config.get(OPTION_LOCAL_FILE_PATH)));
        this.messageFilter = createMessageFilter();

        FileAlterationObserver observer = new FileAlterationObserver(file.getParent());
        observer.addListener(
                new FileAlterationListenerAdaptor() {

                    @Override
                    public void onFileCreate(File file) {
                        fileChanged(file);
                    }

                    @Override
                    public void onFileChange(File file) {
                        fileChanged(file);
                    }

                    @Override
                    public void onFileDelete(File file) {
                        fileChanged(file);
                    }
                });
        monitor = new FileAlterationMonitor(1000, observer);
        monitor.start();
        LOG.info("start to watch file: {}", file);
    }

    protected void fileChanged(File file) {
        if (file.getName().equals(this.file.getName())) {
            LOG.info("file changed: {}", file.getAbsolutePath());
            try {
                this.messageFilter = createMessageFilter();
            } catch (Exception e) {
                LOG.warn("create message filter error: {}", file.getAbsolutePath(), e);
            }
        }
    }

    private MessageFilter<Object> createMessageFilter()
            throws IOException, InstantiationException, IllegalAccessException {
        if (!file.exists() || !file.isFile()) {
            return null;
        }
        try (InputStream is = Files.newInputStream(file.toPath())) {
            final String code = new String(IOUtils.readFully(is), StandardCharsets.UTF_8);
            final Class<MessageFilter<Object>> filterClass = CompileUtils.doCompile(code);
            return filterClass.newInstance();
        }
    }

    @Override
    public MessageFilter<Object> getMessageFilter() {
        return messageFilter;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (monitor != null) {
                try {
                    monitor.stop();
                } catch (Exception e) {
                    LOG.warn("stop monitor error: {}", file.getAbsolutePath(), e);
                }
            }
        }
    }

    @Override
    public String identity() {
        return IDENTITY;
    }

    /**
     * for test.
     *
     * @return boolean
     */
    public boolean isClosed() {
        return closed.get();
    }
}
