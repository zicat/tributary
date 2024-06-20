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

package org.zicat.tributary.source.netty.handler.kafka;

import org.apache.kafka.common.security.plain.PlainAuthenticateCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.util.Map;

/** CallbackHandler. */
public class PlainServerCallbackHandler implements CallbackHandler {

    private final Map<String, String> userPassword;

    public PlainServerCallbackHandler(Map<String, String> userPassword) {
        this.userPassword = userPassword;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        String username = null;
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                username = ((NameCallback) callback).getDefaultName();
            } else if (callback instanceof PlainAuthenticateCallback) {
                PlainAuthenticateCallback plainCallback = (PlainAuthenticateCallback) callback;
                boolean authenticated = authenticate(username, plainCallback.password());
                plainCallback.authenticated(authenticated);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    protected boolean authenticate(String username, char[] password) {
        if (username == null) {
            return false;
        } else {
            final String realPass = userPassword.get(username);
            return new String(password).equals(realPass);
        }
    }
}
