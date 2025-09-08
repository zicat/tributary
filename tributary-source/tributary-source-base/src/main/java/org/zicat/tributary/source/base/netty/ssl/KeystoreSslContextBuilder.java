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

package org.zicat.tributary.source.base.netty.ssl;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import static org.zicat.tributary.common.ResourceUtils.getResourcePath;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import javax.net.ssl.KeyManagerFactory;

/** KeystoreSslContextBuilder. */
public class KeystoreSslContextBuilder extends AbstractSslContextBuilder {

    private String keystoreLocation;
    private String keystorePassword;
    private String keyPassword;
    private KeystoreType keystoreType;

    public KeystoreSslContextBuilder keystoreLocation(String keystoreLocation) {
        this.keystoreLocation = keystoreLocation;
        return this;
    }

    public KeystoreSslContextBuilder keystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public KeystoreSslContextBuilder keyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
        return this;
    }

    public KeystoreSslContextBuilder keystoreType(KeystoreType keystoreType) {
        this.keystoreType = keystoreType;
        return this;
    }

    @Override
    public SslContext buildContext()
            throws KeyStoreException,
                    IOException,
                    CertificateException,
                    NoSuchAlgorithmException,
                    UnrecoverableKeyException {
        if (verifyMode.toClientAuth() != ClientAuth.NONE) {
            throw new IllegalArgumentException(
                    "Client authentication is not supported with keystore based SSL configuration");
        }
        final KeyStore ks = KeyStore.getInstance(keystoreType.name());
        try (InputStream keystoreInputStream =
                Files.newInputStream(new File(getResourcePath(keystoreLocation)).toPath())) {
            ks.load(keystoreInputStream, keystorePassword.toCharArray());
        }
        final KeyManagerFactory kmf =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyPassword.toCharArray());
        return SslContextBuilder.forServer(kmf)
                .protocols(protocols)
                .ciphers(Arrays.asList(DEFAULT_CIPHERS))
                .build();
    }

    /** KeystoreType. */
    public enum KeystoreType {
        JKS
    }
}
