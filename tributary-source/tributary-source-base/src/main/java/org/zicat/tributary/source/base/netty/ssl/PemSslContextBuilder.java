package org.zicat.tributary.source.base.netty.ssl;

import io.netty.handler.ssl.SslContext;

import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLException;

/** PemSslContextBuilder. */
public class PemSslContextBuilder extends AbstractSslContextBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PemSslContextBuilder.class);

    private final File sslKeyFile;
    private final File sslCertificateFile;
    private String[] certificateAuthorities;
    private final String passphrase;

    public PemSslContextBuilder(String sslCertificateFilePath, String sslKeyFilePath, String pass)
            throws IllegalArgumentException {
        sslCertificateFile = new File(sslCertificateFilePath);
        if (!sslCertificateFile.canRead()) {
            throw new IllegalArgumentException(
                    String.format("Certificate file cannot be read %s", sslCertificateFilePath));
        }
        sslKeyFile = new File(sslKeyFilePath);
        if (!sslKeyFile.canRead()) {
            throw new IllegalArgumentException(
                    String.format("Private key file cannot be read %s", sslKeyFilePath));
        }
        passphrase = pass;
    }

    public PemSslContextBuilder setClientAuthentication(
            SslClientVerifyMode verifyMode, String[] certificateAuthorities) {
        if (isClientAuthenticationEnabled(verifyMode)
                && (certificateAuthorities == null || certificateAuthorities.length < 1)) {
            throw new IllegalArgumentException(
                    "Certificate authorities are required to enable client authentication");
        }
        this.verifyMode = verifyMode;
        this.certificateAuthorities = certificateAuthorities;
        return this;
    }

    @Override
    public SslContext buildContext() throws Exception {
        LOG.debug("Available ciphers: {}", SUPPORTED_CIPHERS);
        LOG.debug("Ciphers: {}", Arrays.toString(ciphers));
        SslContextBuilder builder =
                SslContextBuilder.forServer(sslCertificateFile, sslKeyFile, passphrase)
                        .ciphers(Arrays.asList(ciphers))
                        .protocols(protocols);
        if (isClientAuthenticationEnabled(verifyMode)) {
            LOG.debug("Certificate Authorities: {}", Arrays.toString(certificateAuthorities));
            builder.clientAuth(verifyMode.toClientAuth())
                    .trustManager(loadCertificateCollection(certificateAuthorities));
        }

        try {
            return builder.build();
        } catch (SSLException e) {
            LOG.debug("Failed to initialize SSL", e);
            // unwrap generic wrapped exception from Netty's JdkSsl{Client|Server}Context
            if ("failed to initialize the server-side SSL context".equals(e.getMessage())
                    || "failed to initialize the client-side SSL context".equals(e.getMessage())) {
                // Netty catches Exception and simply wraps: throw new SSLException("...", e);
                if (e.getCause() instanceof Exception) {
                    throw (Exception) e.getCause();
                }
            }
            throw e;
        } catch (Exception e) {
            LOG.debug("Failed to initialize SSL", e);
            throw e;
        }
    }

    @SuppressWarnings({"unchecked", "ToArrayCallWithZeroLengthArrayArgument"})
    private X509Certificate[] loadCertificateCollection(String[] certificates)
            throws IOException, CertificateException {
        LOG.debug("Load certificates collection");
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

        final List<X509Certificate> collections = new ArrayList<>();
        for (String certificate : certificates) {
            LOG.debug("Loading certificates from file {}", certificate);

            try (InputStream in = Files.newInputStream(Paths.get(certificate))) {
                List<X509Certificate> certificatesChains =
                        (List<X509Certificate>) certificateFactory.generateCertificates(in);
                collections.addAll(certificatesChains);
            }
        }
        return collections.toArray(new X509Certificate[collections.size()]);
    }

    public String[] getCertificateAuthorities() {
        return certificateAuthorities != null ? certificateAuthorities.clone() : null;
    }
}
