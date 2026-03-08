package com.example.nettyfileserver.util;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.StandardCharsets;

/**
 * 文本响应与空响应的公共封装，统一 CORS/连接关闭策略。
 */
public final class HttpResponseUtil {
    private HttpResponseUtil() {
    }

    public static void sendText(ChannelHandlerContext ctx, HttpResponseStatus status, String body, boolean keepAlive) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.wrappedBuffer(bytes)
        );

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        CorsUtil.applyCors(response.headers());
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        // 非 keep-alive 请求在响应发送完后主动断开连接。
        ChannelFuture future = ctx.writeAndFlush(response);
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    public static void sendNoContent(ChannelHandlerContext ctx, boolean keepAlive) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.NO_CONTENT
        );
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        CorsUtil.applyCors(response.headers());
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        // OPTIONS 预检等场景会走这里，规则与 sendText 保持一致。
        ChannelFuture future = ctx.writeAndFlush(response);
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
