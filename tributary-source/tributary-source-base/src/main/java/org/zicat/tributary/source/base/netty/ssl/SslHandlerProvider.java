package org.zicat.tributary.source.base.netty.ssl;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

/** SslHandlerProvider. */
public class SslHandlerProvider {

    private final SslContext sslContext;
    private final long sslHandshakeTimeoutMillis;

    public SslHandlerProvider(SslContext context, long sslHandshakeTimeoutMillis) {
        this.sslContext = context;
        this.sslHandshakeTimeoutMillis = sslHandshakeTimeoutMillis;
    }

    public SslHandler sslHandlerForChannel(final Channel socket) {
        final SslHandler handler = sslContext.newHandler(socket.alloc());
        handler.setHandshakeTimeoutMillis(sslHandshakeTimeoutMillis);
        return handler;
    }
}
