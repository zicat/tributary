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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.crypto.Cipher;
import javax.net.ssl.SSLServerSocketFactory;

/** SslContextBuilderBase. */
public abstract class AbstractSslContextBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSslContextBuilder.class);
    public static final String[] SUPPORT_PROTOCOL = new String[] {"TLSv1.2", "TLSv1.3"};

    public static final Set<String> SUPPORTED_CIPHERS =
            new HashSet<>(
                    Arrays.asList(
                            ((SSLServerSocketFactory) SSLServerSocketFactory.getDefault())
                                    .getSupportedCipherSuites()));

    public static final String[] DEFAULT_CIPHERS;

    static {
        String[] defaultCipherCandidates =
                new String[] {
                    "TLS_AES_256_GCM_SHA384", // TLS 1.3
                    "TLS_AES_128_GCM_SHA256", // TLS 1.3
                    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
                    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                };
        DEFAULT_CIPHERS =
                Arrays.stream(defaultCipherCandidates)
                        .filter(SUPPORTED_CIPHERS::contains)
                        .toArray(String[]::new);
    }

    /*
     Reduced set of ciphers available when JCE Unlimited Strength Jurisdiction Policy is not installed.
    */
    public static final String[] DEFAULT_CIPHERS_LIMITED =
            new String[] {
                "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"
            };

    protected String[] ciphers = DEFAULT_CIPHERS;
    protected String[] protocols = SUPPORT_PROTOCOL;
    protected SslClientVerifyMode verifyMode = SslClientVerifyMode.NONE;

    public AbstractSslContextBuilder protocols(List<String> protocols) {
        if (protocols == null || protocols.isEmpty()) {
            return this;
        }
        return protocols(protocols.toArray(new String[] {}));
    }

    public AbstractSslContextBuilder protocols(String[] protocols) {
        if (protocols == null || protocols.length == 0) {
            return this;
        }
        this.protocols = protocols;
        return this;
    }

    public AbstractSslContextBuilder verifyMode(SslClientVerifyMode verifyMode) {
        this.verifyMode = verifyMode;
        return this;
    }

    public abstract SslContext buildContext() throws Exception;

    /** SslClientVerifyMode. */
    public enum SslClientVerifyMode {
        NONE(ClientAuth.NONE),
        OPTIONAL(ClientAuth.OPTIONAL),
        REQUIRED(ClientAuth.REQUIRE);

        private final ClientAuth clientAuth;

        SslClientVerifyMode(ClientAuth clientAuth) {
            this.clientAuth = clientAuth;
        }

        public ClientAuth toClientAuth() {
            return clientAuth;
        }
    }

    public void setCipherSuites(String[] ciphersSuite) throws IllegalArgumentException {
        for (String cipher : ciphersSuite) {
            if (SUPPORTED_CIPHERS.contains(cipher)) {
                LOG.debug("{} cipher is supported", cipher);
            } else {
                if (!isUnlimitedJCEAvailable()) {
                    LOG.warn("JCE Unlimited Strength Jurisdiction Policy not installed");
                }
                throw new IllegalArgumentException("Cipher `" + cipher + "` is not available");
            }
        }
        ciphers = ciphersSuite;
    }

    public static boolean isUnlimitedJCEAvailable() {
        try {
            return (Cipher.getMaxAllowedKeyLength("AES") > 128);
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("AES not available", e);
            return false;
        }
    }

    public static String[] getDefaultCiphers() {
        if (isUnlimitedJCEAvailable()) {
            return DEFAULT_CIPHERS;
        } else {
            LOG.warn(
                    "JCE Unlimited Strength Jurisdiction Policy not installed - max key length is 128 bits");
            return DEFAULT_CIPHERS_LIMITED;
        }
    }

    protected boolean isClientAuthenticationEnabled(final SslClientVerifyMode mode) {
        return mode == SslClientVerifyMode.OPTIONAL || mode == SslClientVerifyMode.REQUIRED;
    }

    public boolean isClientAuthenticationRequired() {
        return verifyMode == SslClientVerifyMode.REQUIRED;
    }

    /**
     * Get the supported protocols.
     *
     * @return a defensive copy of the supported protocols
     */
    public String[] getProtocols() {
        return protocols != null ? protocols.clone() : null;
    }

    public String[] getCiphers() {
        return ciphers != null ? ciphers.clone() : null;
    }

    public SslClientVerifyMode getVerifyMode() {
        return verifyMode;
    }
}
