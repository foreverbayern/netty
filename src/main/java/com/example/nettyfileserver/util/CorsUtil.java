package com.example.nettyfileserver.util;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;

public final class CorsUtil {
    private CorsUtil() {
    }

    public static void applyCors(HttpHeaders headers) {
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET,POST,OPTIONS");
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type,Content-Length,Accept,Origin,User-Agent,Cache-Control,X-Requested-With");
        headers.set(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, "Content-Disposition,Content-Length,Content-Type");
        headers.set(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, "86400");
    }
}
