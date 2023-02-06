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

package org.zicat.tributary.sink.utils;

import org.zicat.tributary.common.TributaryRuntimeException;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/** host utils. */
public class HostUtils {

    /**
     * get localhost string ip by pattern filter.
     *
     * @return host
     */
    public static String getLocalHostString(String metricsIpPattern) {
        return getLocalInetAddress(metricsIpPattern).getHostAddress();
    }

    /**
     * get localhost string ip by pattern filter.
     *
     * @return host
     */
    public static InetAddress getLocalInetAddress(String metricsIpPattern) {
        final Enumeration<NetworkInterface> it;
        try {
            it = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new TributaryRuntimeException(e);
        }
        while (it.hasMoreElements()) {
            final NetworkInterface networkInterface = it.nextElement();
            final Enumeration<InetAddress> addressIt = networkInterface.getInetAddresses();
            while (addressIt.hasMoreElements()) {
                InetAddress inetAddress = addressIt.nextElement();
                if (!inetAddress.isLoopbackAddress()
                        && inetAddress instanceof Inet4Address
                        && (metricsIpPattern == null
                                || inetAddress.getHostAddress().matches(metricsIpPattern))) {
                    return inetAddress;
                }
            }
        }
        throw new TributaryRuntimeException("local ip not found");
    }
}
