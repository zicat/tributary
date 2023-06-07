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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

/** LengthBodyHDFSWriterFactory. */
public class LengthBodyHDFSWriterFactory implements HDFSWriterFactory {

    private final CompressionCodec compressionCodec;

    public LengthBodyHDFSWriterFactory()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        this(null);
    }

    public LengthBodyHDFSWriterFactory(String codec)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.compressionCodec = createCodec(codec);
    }

    /**
     * create codec by name.
     *
     * @param codec codec
     * @return CompressionCodec
     * @throws ClassNotFoundException ClassNotFoundException
     * @throws IllegalAccessException IllegalAccessException
     * @throws InstantiationException InstantiationException
     */
    private static CompressionCodec createCodec(String codec)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (codec == null || codec.trim().isEmpty()) {
            return null;
        }
        final CompressionCodec compressionCodec =
                (CompressionCodec) Class.forName(codec).newInstance();
        if (compressionCodec instanceof Configurable) {
            ((Configurable) compressionCodec).setConf(new Configuration());
        }
        return compressionCodec;
    }

    @Override
    public String fileExtension() {
        return compressionCodec == null ? "" : compressionCodec.getDefaultExtension();
    }

    @Override
    public HDFSWriter create() {
        return compressionCodec == null
                ? new LengthBodyHDFSWriter()
                : new LengthBodyCompressionHDFSWriter(compressionCodec);
    }
}
