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

package org.zicat.tributary.common.util;

import org.zicat.tributary.common.exception.TributaryRuntimeException;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

/** host utils. */
public class HostUtils {

    /**
     * get localhost string ip by pattern filter.
     *
     * @param pattern pattern
     * @return string host
     */
    public static String getFirstMatchedHostAddress(String pattern) {
        return getHostAddresses(pattern).get(0);
    }

    /**
     * get host addresses.
     *
     * @param pattern pattern
     * @return hosts
     */
    public static List<String> getHostAddresses(String pattern) {
        return getInetAddress(pattern).stream()
                .map(InetAddress::getHostAddress)
                .collect(Collectors.toList());
    }

    /**
     * get localhost string ip by pattern filter.
     *
     * @return host
     */
    private static List<InetAddress> getInetAddress(String pattern) {
        final List<InetAddress> inetAddresses = new ArrayList<>();
        if (pattern == null) {
            return inetAddresses;
        }
        if ("localhost".matches(pattern) || "127.0.0.1".matches(pattern)) {
            inetAddresses.add(InetAddress.getLoopbackAddress());
        }
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
                final InetAddress inetAddress = addressIt.nextElement();
                if (!inetAddress.isLoopbackAddress()
                        && inetAddress instanceof Inet4Address
                        && inetAddress.getHostAddress().matches(pattern)) {
                    inetAddresses.add(inetAddress);
                }
            }
        }
        return inetAddresses;
    }
}
