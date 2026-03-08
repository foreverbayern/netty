package com.example.nettyfileserver.handler;

import com.example.nettyfileserver.util.CorsUtil;
import com.example.nettyfileserver.util.HttpResponseUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.stream.ChunkedFile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 下载处理器：负责读取目标文件并按 HTTP chunked 方式回传给客户端。
 */
public class DownloadHandler {
    // 分块发送尺寸，兼顾吞吐与单次内存占用。
    private static final int CHUNK_SIZE = 8192;

    private final Path dataDir;

    public DownloadHandler(Path dataDir) {
        this.dataDir = dataDir;
    }

    public void handleDownload(ChannelHandlerContext ctx, HttpRequest request, String filename) {
        Path filePath = dataDir.resolve(filename).normalize();
        if (!Files.isRegularFile(filePath)) {
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.NOT_FOUND, "File not found\n", HttpUtil.isKeepAlive(request));
            return;
        }

        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(filePath.toFile(), "r");
            long fileLength = raf.length();

            DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            HttpUtil.setContentLength(response, fileLength);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
            response.headers().set(HttpHeaderNames.CONTENT_DISPOSITION, buildContentDisposition(filename));
            CorsUtil.applyCors(response.headers());

            boolean keepAlive = HttpUtil.isKeepAlive(request);
            if (keepAlive) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            // 先回响应头，再流式发送文件内容，避免大文件一次性进内存。
            ctx.write(response);
            ChannelFuture sendFileFuture = ctx.writeAndFlush(
                    new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, CHUNK_SIZE))
            );
            RandomAccessFile finalRaf = raf;
            sendFileFuture.addListener(future -> closeQuietly(finalRaf));
            if (!keepAlive) {
                sendFileFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (IOException ex) {
            closeQuietly(raf);
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Download failed\n", false);
        }
    }

    private static void closeQuietly(RandomAccessFile raf) {
        if (raf == null) {
            return;
        }
        try {
            raf.close();
        } catch (IOException ignored) {
            // 下载尾声阶段关闭失败通常无恢复价值，直接忽略。
        }
    }

    private static String buildContentDisposition(String filename) {
        // 按 RFC 5987 返回 UTF-8 文件名，并提供 ASCII 兜底以兼容旧客户端。
        String asciiFallback = filename
                .replace("\"", "_")
                .replace("\r", "_")
                .replace("\n", "_")
                .replaceAll("[^\\x20-\\x7E]", "_");
        if (asciiFallback.isBlank()) {
            asciiFallback = "download";
        }

        String encoded = URLEncoder.encode(filename, StandardCharsets.UTF_8).replace("+", "%20");
        return "attachment; filename=\"" + asciiFallback + "\"; filename*=UTF-8''" + encoded;
    }
}
