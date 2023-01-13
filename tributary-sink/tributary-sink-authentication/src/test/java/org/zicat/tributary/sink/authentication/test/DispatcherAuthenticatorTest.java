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

package org.zicat.tributary.sink.authentication.test;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.test.utils.FileUtils;
import org.zicat.tributary.sink.authentication.*;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import static org.junit.Assert.*;

/** DispatcherAuthenticatorTest. */
public class DispatcherAuthenticatorTest {

    private static MiniKdc kdc;
    private static File workDir;
    private static File flumeKeytab;
    private static String flumePrincipal = "flume/localhost";
    private static File aliceKeytab;
    private static String alicePrincipal = "alice";

    @BeforeClass
    public static void startMiniKdc() throws Exception {
        workDir = FileUtils.createTmpDir("test_mini_kdc");
        flumeKeytab = new File(workDir, "flume.keytab");
        aliceKeytab = new File(workDir, "alice.keytab");
        Properties conf = MiniKdc.createConf();

        kdc = new MiniKdc(conf, workDir);
        kdc.start();

        kdc.createPrincipal(flumeKeytab, flumePrincipal);
        flumePrincipal = flumePrincipal + "@" + kdc.getRealm();

        kdc.createPrincipal(aliceKeytab, alicePrincipal);
        alicePrincipal = alicePrincipal + "@" + kdc.getRealm();
    }

    @AfterClass
    public static void stopMiniKdc() {
        if (kdc != null) {
            kdc.stop();
        }
    }

    @After
    public void tearDown() {
        // Clear the previous statically stored logged in credentials
        DispatcherAuthenticationUtil.clearCredentials();
    }

    @Test
    public void testFlumeLogin() {
        String principal = flumePrincipal;
        String keytab = flumeKeytab.getAbsolutePath();
        String expResult = principal;

        DispatcherAuthenticator authenticator =
                DispatcherAuthenticationUtil.getAuthenticator(principal, keytab);
        assertTrue(authenticator.isAuthenticated());

        String result = ((KerberosAuthenticator) authenticator).getUserName();
        assertEquals("Initial login failed", expResult, result);

        authenticator = DispatcherAuthenticationUtil.getAuthenticator(principal, keytab);
        result = ((KerberosAuthenticator) authenticator).getUserName();
        assertEquals("Re-login failed", expResult, result);

        principal = alicePrincipal;
        keytab = aliceKeytab.getAbsolutePath();
        try {
            authenticator = DispatcherAuthenticationUtil.getAuthenticator(principal, keytab);
            result = ((KerberosAuthenticator) authenticator).getUserName();
            fail("Login should have failed with a new principal: " + result);
        } catch (Exception ex) {
            assertTrue(
                    "Login with a new principal failed, but for an unexpected "
                            + "reason: "
                            + ex.getMessage(),
                    ex.getMessage().contains("Cannot use multiple kerberos principals"));
        }
    }

    /**
     * Test whether the exception raised in the <code>PrivilegedExceptionAction</code> gets
     * propagated as-is from {@link KerberosAuthenticator#execute(PrivilegedExceptionAction)}.
     */
    @Test(expected = IOException.class)
    public void testKerberosAuthenticatorExceptionInExecute() throws Exception {
        String principal = flumePrincipal;
        String keytab = flumeKeytab.getAbsolutePath();

        DispatcherAuthenticator authenticator =
                DispatcherAuthenticationUtil.getAuthenticator(principal, keytab);
        assertTrue(authenticator instanceof KerberosAuthenticator);

        authenticator.execute(
                (PrivilegedExceptionAction<Object>)
                        () -> {
                            throw new IOException();
                        });
    }

    @Test
    public void testNullLogin() {
        DispatcherAuthenticator authenticator =
                DispatcherAuthenticationUtil.getAuthenticator(null, null);
        assertFalse(authenticator.isAuthenticated());
    }

    /**
     * Test whether the exception raised in the <code>PrivilegedExceptionAction</code> gets
     * propagated as-is from {@link SimpleAuthenticator#execute(PrivilegedExceptionAction)}.
     */
    @Test(expected = IOException.class)
    public void testSimpleAuthenticatorExceptionInExecute() throws Exception {
        DispatcherAuthenticator authenticator =
                DispatcherAuthenticationUtil.getAuthenticator(null, null);
        assertTrue(authenticator instanceof SimpleAuthenticator);

        authenticator.execute(
                (PrivilegedExceptionAction<Object>)
                        () -> {
                            throw new IOException();
                        });
    }

    @Test
    public void testProxyAs() {
        String username = "alice";

        DispatcherAuthenticator authenticator =
                DispatcherAuthenticationUtil.getAuthenticator(null, null);
        String result = ((UGIExecutor) (authenticator.proxyAs(username))).getUserName();
        assertEquals("Proxy as didn't generate the expected username", username, result);

        authenticator =
                DispatcherAuthenticationUtil.getAuthenticator(
                        flumePrincipal, flumeKeytab.getAbsolutePath());

        String login = ((KerberosAuthenticator) authenticator).getUserName();
        assertEquals("Login succeeded, but the principal doesn't match", flumePrincipal, login);

        result = ((UGIExecutor) (authenticator.proxyAs(username))).getUserName();
        assertEquals("Proxy as didn't generate the expected username", username, result);
    }

    @Test
    public void testFlumeLoginPrincipalWithoutRealm() throws Exception {
        String principal = "flume";
        File keytab = new File(workDir, "flume2.keytab");
        kdc.createPrincipal(keytab, principal);
        String expResult = principal + "@" + kdc.getRealm();

        DispatcherAuthenticator authenticator =
                DispatcherAuthenticationUtil.getAuthenticator(principal, keytab.getAbsolutePath());
        assertTrue(authenticator.isAuthenticated());

        String result = ((KerberosAuthenticator) authenticator).getUserName();
        assertEquals("Initial login failed", expResult, result);

        authenticator =
                DispatcherAuthenticationUtil.getAuthenticator(principal, keytab.getAbsolutePath());
        result = ((KerberosAuthenticator) authenticator).getUserName();
        assertEquals("Re-login failed", expResult, result);

        principal = "alice";
        keytab = aliceKeytab;
        try {
            authenticator =
                    DispatcherAuthenticationUtil.getAuthenticator(
                            principal, keytab.getAbsolutePath());
            result = ((KerberosAuthenticator) authenticator).getUserName();
            fail("Login should have failed with a new principal: " + result);
        } catch (Exception ex) {
            assertTrue(
                    "Login with a new principal failed, but for an unexpected "
                            + "reason: "
                            + ex.getMessage(),
                    ex.getMessage().contains("Cannot use multiple kerberos principals"));
        }
    }
}
