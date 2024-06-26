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

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.types.Struct;

/** ApiError. */
public class ApiError {
    public static final ApiError NONE = new ApiError(Errors.NONE, null);

    private final Errors error;
    private final String message;

    public static ApiError fromThrowable(Throwable t) {
        // Avoid populating the error message if it's a generic one
        Errors error = Errors.forException(t);
        String message = error.message().equals(t.getMessage()) ? null : t.getMessage();
        return new ApiError(error, message);
    }

    public ApiError(Struct struct) {
        error = Errors.forCode(struct.get(CommonFields.ERROR_CODE));
        // In some cases, the error message field was introduced in newer version
        message = struct.getOrElse(CommonFields.ERROR_MESSAGE, null);
    }

    public ApiError(Errors error) {
        this(error, error.message());
    }

    public ApiError(Errors error, String message) {
        this.error = error;
        this.message = message;
    }

    public void write(Struct struct) {
        struct.set(CommonFields.ERROR_CODE, error.code());
        if (error != Errors.NONE) {
            struct.setIfExists(CommonFields.ERROR_MESSAGE, message);
        }
    }

    public boolean is(Errors error) {
        return this.error == error;
    }

    public boolean isFailure() {
        return !isSuccess();
    }

    public boolean isSuccess() {
        return is(Errors.NONE);
    }

    public Errors error() {
        return error;
    }

    /**
     * Return the optional error message or null. Consider using {@link #messageWithFallback()}
     * instead.
     */
    public String message() {
        return message;
    }

    /**
     * If `message` is defined, return it. Otherwise fallback to the default error message
     * associated with the error code.
     */
    public String messageWithFallback() {
        if (message == null) {
            return error.message();
        }
        return message;
    }

    public ApiException exception() {
        return error.exception(message);
    }

    @Override
    public String toString() {
        return "ApiError(error=" + error + ", message=" + message + ")";
    }
}
