package com.example.nettyfileserver.handler;

import com.example.nettyfileserver.util.FilenameValidator;
import com.example.nettyfileserver.util.HttpResponseUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class HttpRouterHandler extends SimpleChannelInboundHandler<HttpObject> {
    private final UploadHandler uploadHandler;
    private final DownloadHandler downloadHandler;

    private boolean uploadInProgress;

    public HttpRouterHandler(Path dataDir) {
        this.uploadHandler = new UploadHandler(dataDir);
        this.downloadHandler = new DownloadHandler(dataDir);
        this.uploadInProgress = false;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
//        EventExecutor executor = ctx.executor();
//        System.out.println(executor.inEventLoop());
//        System.out.println(Thread.currentThread().getName());
        if (msg instanceof HttpRequest request) {
            handleRequest(ctx, request);
        }

        if (msg instanceof HttpContent content) {
            handleContent(ctx, content);
        }
    }

    private void handleRequest(ChannelHandlerContext ctx, HttpRequest request) {
        if (uploadInProgress) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Previous upload is not finished\n", false);
            return;
        }

        if (HttpMethod.OPTIONS.equals(request.method())) {
            HttpResponseUtil.sendNoContent(ctx, HttpUtil.isKeepAlive(request));
            return;
        }

        QueryStringDecoder query = new QueryStringDecoder(request.uri());
        String path = query.path();

        if (HttpMethod.POST.equals(request.method()) && "/upload".equals(path)) {
            handleUploadRequest(ctx, request, query.parameters());
            return;
        }

        if (HttpMethod.GET.equals(request.method()) && "/download".equals(path)) {
            handleDownloadRequest(ctx, request, query.parameters());
            return;
        }

        HttpResponseUtil.sendText(ctx, HttpResponseStatus.NOT_FOUND, "Endpoint not found\n", HttpUtil.isKeepAlive(request));
    }

    private void handleUploadRequest(ChannelHandlerContext ctx, HttpRequest request, Map<String, List<String>> params) {
        String filename = firstParam(params, "filename");
        boolean keepAlive = HttpUtil.isKeepAlive(request);

        if (!FilenameValidator.isSafeFilename(filename)) {
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid filename\n", keepAlive);
            return;
        }

        if (HttpUtil.is100ContinueExpected(request)) {
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }

        try {
            uploadHandler.beginUpload(filename, keepAlive);
            uploadInProgress = true;
        } catch (IOException ex) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to open upload target\n", false);
        }
    }

    private void handleDownloadRequest(ChannelHandlerContext ctx, HttpRequest request, Map<String, List<String>> params) {
        String filename = firstParam(params, "filename");
        if (!FilenameValidator.isSafeFilename(filename)) {
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid filename\n", HttpUtil.isKeepAlive(request));
            return;
        }

        downloadHandler.handleDownload(ctx, request, filename);
    }

    private void handleContent(ChannelHandlerContext ctx, HttpContent content) {
        if (!uploadInProgress) {
            // Many normal requests include EMPTY_LAST_CONTENT at the end; ignore it when no upload is active.
            if (content instanceof LastHttpContent && !content.content().isReadable()) {
                return;
            }
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Unexpected request body\n", false);
            return;
        }

        try {
            UploadHandler.UploadResult result = uploadHandler.handleChunk(content);
            if (result != null) {
                uploadInProgress = false;
                String message = "Upload successful: " + result.filename() + ", bytes=" + result.bytesWritten() + "\n";
                HttpResponseUtil.sendText(ctx, HttpResponseStatus.OK, message, result.keepAlive());
            }
        } catch (IllegalStateException ex) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "No active upload\n", false);
        } catch (IOException ex) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Upload failed\n", false);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        uploadHandler.abortAndCleanup();
        uploadInProgress = false;
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        uploadHandler.abortAndCleanup();
        uploadInProgress = false;
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error\n", false);
    }

    private static String firstParam(Map<String, List<String>> params, String name) {
        List<String> values = params.get(name);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }
}
