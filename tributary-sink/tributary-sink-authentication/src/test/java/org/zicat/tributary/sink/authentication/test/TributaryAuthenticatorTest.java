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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.common.test.util.FileUtils;
import org.zicat.tributary.sink.authentication.*;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.zicat.tributary.sink.authentication.KerberosAuthenticatorFactory.getOrCreateKerberosExecutor;

/** DispatcherAuthenticatorTest. */
public class TributaryAuthenticatorTest {

    private static MiniKdc kdc;
    private static File workDir;
    private static File zicatKeytab;
    private static String zicatPrincipal = "zicat/localhost";
    private static File aliceKeytab;
    private static String alicePrincipal = "alice";

    @BeforeClass
    public static void startMiniKdc() throws Exception {
        workDir = FileUtils.createTmpDir("test_mini_kdc");
        zicatKeytab = new File(workDir, "zicat.keytab");
        aliceKeytab = new File(workDir, "alice.keytab");
        Properties conf = MiniKdc.createConf();

        kdc = new MiniKdc(conf, workDir);
        kdc.start();

        kdc.createPrincipal(zicatKeytab, zicatPrincipal);
        zicatPrincipal = zicatPrincipal + "@" + kdc.getRealm();

        kdc.createPrincipal(aliceKeytab, alicePrincipal);
        alicePrincipal = alicePrincipal + "@" + kdc.getRealm();
    }

    @AfterClass
    public static void stopMiniKdc() {
        if (kdc != null) {
            kdc.stop();
        }
    }

    @Test
    public void testZicatLogin() throws IOException {
        String principal = zicatPrincipal;
        String keytab = zicatKeytab.getAbsolutePath();
        String expResult = principal;

        PrivilegedExecutor authenticator = getOrCreateKerberosExecutor(principal, keytab);

        String result = ((UGIPrivilegedExecutor) authenticator).getUser();
        assertEquals("Initial login failed", expResult, result);

        authenticator = getOrCreateKerberosExecutor(principal, keytab);
        result = ((UGIPrivilegedExecutor) authenticator).getUser();
        assertEquals("Re-login failed", expResult, result);

        principal = alicePrincipal;
        keytab = aliceKeytab.getAbsolutePath();
        authenticator = getOrCreateKerberosExecutor(principal, keytab);
        result = ((UGIPrivilegedExecutor) authenticator).getUser();
        Assert.assertNotNull(result);
    }

    @Test(expected = IOException.class)
    public void testKerberosAuthenticatorExceptionInExecute() throws Exception {
        String principal = zicatPrincipal;
        String keytab = zicatKeytab.getAbsolutePath();

        PrivilegedExecutor authenticator = getOrCreateKerberosExecutor(principal, keytab);
        assertTrue(authenticator instanceof UGIPrivilegedExecutor);

        authenticator.execute(
                (PrivilegedExceptionAction<Object>)
                        () -> {
                            throw new IOException();
                        });
    }

    @Test
    public void testNullLogin() throws IOException {
        getOrCreateKerberosExecutor(null, null);
    }

    /**
     * Test whether the exception raised in the <code>PrivilegedExceptionAction</code> gets
     * propagated as-is from {@link SimplePrivilegedExecutor#execute(PrivilegedExceptionAction)}.
     */
    @Test(expected = IOException.class)
    public void testSimpleAuthenticatorExceptionInExecute() throws Exception {
        PrivilegedExecutor authenticator = getOrCreateKerberosExecutor(null, null);
        assertTrue(authenticator instanceof SimplePrivilegedExecutor);
        authenticator.execute(
                (PrivilegedExceptionAction<Object>)
                        () -> {
                            throw new IOException();
                        });
    }

    @Test
    public void testZicatLoginPrincipalWithoutRealm() throws Exception {
        String principal = "zicat";
        File keytab = new File(workDir, "zicat2.keytab");
        kdc.createPrincipal(keytab, principal);
        String expResult = principal + "@" + kdc.getRealm();

        PrivilegedExecutor authenticator =
                getOrCreateKerberosExecutor(principal, keytab.getAbsolutePath());

        String result = ((UGIPrivilegedExecutor) authenticator).getUser();
        assertEquals("Initial login failed", expResult, result);

        authenticator = getOrCreateKerberosExecutor(principal, keytab.getAbsolutePath());
        result = ((UGIPrivilegedExecutor) authenticator).getUser();
        assertEquals("Re-login failed", expResult, result);

        principal = "alice";
        keytab = aliceKeytab;
        authenticator = getOrCreateKerberosExecutor(principal, keytab.getAbsolutePath());
        result = ((UGIPrivilegedExecutor) authenticator).getUser();
        assertEquals(
                "Login should have failed with a new principal: " + result,
                principal + "@" + kdc.getRealm(),
                result);
    }
}
