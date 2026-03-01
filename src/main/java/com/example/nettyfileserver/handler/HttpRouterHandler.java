package com.example.nettyfileserver.handler;

import com.example.nettyfileserver.util.FilenameValidator;
import com.example.nettyfileserver.util.HttpResponseUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.List;
import java.util.Map;

public class HttpRouterHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final long MAX_UPLOAD_BYTES = 200L * 1024 * 1024;

    private final UploadHandler uploadHandler;
    private final UploadHandler asyncUploadHandler;
    private final DownloadHandler downloadHandler;
    private final ExecutorService uploadIoExecutor;

    private boolean uploadInProgress;
    private boolean asyncUploadInProgress;
    private long uploadBytesReceived;
    private long asyncUploadBytesReceived;
    private CompletableFuture<Void> asyncUploadChain;

    public HttpRouterHandler(Path dataDir, ExecutorService uploadIoExecutor) {
        this.uploadHandler = new UploadHandler(dataDir);
        this.asyncUploadHandler = new UploadHandler(dataDir);
        this.downloadHandler = new DownloadHandler(dataDir);
        this.uploadIoExecutor = uploadIoExecutor;
        this.uploadInProgress = false;
        this.asyncUploadInProgress = false;
        this.uploadBytesReceived = 0L;
        this.asyncUploadBytesReceived = 0L;
        this.asyncUploadChain = CompletableFuture.completedFuture(null);
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
        if (uploadInProgress || asyncUploadInProgress) {
            abortSyncUpload();
            abortAsyncUpload();
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

        if (HttpMethod.POST.equals(request.method()) && "/upload-async".equals(path)) {
            handleAsyncUploadRequest(ctx, request, query.parameters());
            return;
        }

        if (HttpMethod.GET.equals(request.method()) && "/ping".equals(path)) {
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.OK, "ok", HttpUtil.isKeepAlive(request));
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
        if (hasDeclaredLengthTooLarge(request)) {
            HttpResponseUtil.sendText(
                    ctx,
                    HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                    buildUploadTooLargeMessage(),
                    false
            );
            return;
        }
        if (HttpUtil.is100ContinueExpected(request)) {
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }

        try {
            uploadHandler.beginUpload(filename, keepAlive);
            uploadInProgress = true;
            uploadBytesReceived = 0L;
        } catch (IOException ex) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            uploadBytesReceived = 0L;
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

    private void handleAsyncUploadRequest(ChannelHandlerContext ctx, HttpRequest request, Map<String, List<String>> params) {
        String filename = firstParam(params, "filename");
        boolean keepAlive = HttpUtil.isKeepAlive(request);

        if (!FilenameValidator.isSafeFilename(filename)) {
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid filename\n", keepAlive);
            return;
        }
        if (hasDeclaredLengthTooLarge(request)) {
            HttpResponseUtil.sendText(
                    ctx,
                    HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                    buildUploadTooLargeMessage(),
                    false
            );
            return;
        }
        if (HttpUtil.is100ContinueExpected(request)) {
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }

        asyncUploadInProgress = true;
        asyncUploadBytesReceived = 0L;
        enqueueAsyncUploadTask(ctx, "Failed to open upload target\n", () -> asyncUploadHandler.beginUpload(filename, keepAlive));
    }

    private void handleContent(ChannelHandlerContext ctx, HttpContent content) {
        if (uploadInProgress) {
            handleSyncUploadContent(ctx, content);
            return;
        }

        if (asyncUploadInProgress) {
            handleAsyncUploadContent(ctx, content);
            return;
        }

        // Many normal requests include EMPTY_LAST_CONTENT at the end; ignore it when no upload is active.
        if (content instanceof LastHttpContent && !content.content().isReadable()) {
            return;
        }
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Unexpected request body\n", false);
    }

    private void handleSyncUploadContent(ChannelHandlerContext ctx, HttpContent content) {
        int chunkBytes = content.content().readableBytes();
        if (exceedsLimit(uploadBytesReceived, chunkBytes)) {
            abortSyncUpload();
            HttpResponseUtil.sendText(
                    ctx,
                    HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                    buildUploadTooLargeMessage(),
                    false
            );
            return;
        }
        uploadBytesReceived += chunkBytes;

        try {
            UploadHandler.UploadResult result = uploadHandler.handleChunk(content);
            if (result != null) {
                uploadInProgress = false;
                uploadBytesReceived = 0L;
                String message = "Upload successful: " + result.filename() + ", bytes=" + result.bytesWritten() + "\n";
                HttpResponseUtil.sendText(ctx, HttpResponseStatus.OK, message, result.keepAlive());
            }
        } catch (IllegalStateException ex) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            uploadBytesReceived = 0L;
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "No active upload\n", false);
        } catch (IOException ex) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            uploadBytesReceived = 0L;
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Upload failed\n", false);
        }
    }

    private void handleAsyncUploadContent(ChannelHandlerContext ctx, HttpContent content) {
        int chunkBytes = content.content().readableBytes();
        if (exceedsLimit(asyncUploadBytesReceived, chunkBytes)) {
            abortAsyncUpload();
            HttpResponseUtil.sendText(
                    ctx,
                    HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                    buildUploadTooLargeMessage(),
                    false
            );
            return;
        }
        asyncUploadBytesReceived += chunkBytes;

        byte[] bytes = new byte[chunkBytes];
        content.content().getBytes(content.content().readerIndex(), bytes);
        boolean lastChunk = content instanceof LastHttpContent;

        enqueueAsyncUploadTask(ctx, "Upload failed\n", () -> {
            UploadHandler.UploadResult result = asyncUploadHandler.handleChunkBytes(bytes, lastChunk);
            if (result != null) {
                ctx.executor().execute(() -> completeAsyncUpload(ctx, result));
            }
        });
    }

    private void enqueueAsyncUploadTask(ChannelHandlerContext ctx, String errorMessage, UploadIoTask task) {
        asyncUploadChain = asyncUploadChain
                .thenRunAsync(() -> runUploadTask(task), uploadIoExecutor)
                .whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        ctx.executor().execute(() -> failAsyncUpload(ctx, errorMessage));
                    }
                });
    }

    private static void runUploadTask(UploadIoTask task) {
        try {
            task.run();
        } catch (IOException ex) {
            throw new CompletionException(ex);
        }
    }

    private void completeAsyncUpload(ChannelHandlerContext ctx, UploadHandler.UploadResult result) {
        if (!asyncUploadInProgress) {
            return;
        }
        asyncUploadInProgress = false;
        asyncUploadBytesReceived = 0L;
        asyncUploadChain = CompletableFuture.completedFuture(null);
        String message = "Upload successful: " + result.filename() + ", bytes=" + result.bytesWritten() + "\n";
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.OK, message, result.keepAlive());
    }

    private void failAsyncUpload(ChannelHandlerContext ctx, String errorMessage) {
        if (!asyncUploadInProgress) {
            return;
        }
        asyncUploadInProgress = false;
        asyncUploadBytesReceived = 0L;
        asyncUploadChain = CompletableFuture.completedFuture(null);
        CompletableFuture.runAsync(asyncUploadHandler::abortAndCleanup, uploadIoExecutor);
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, errorMessage, false);
    }

    private void abortSyncUpload() {
        uploadBytesReceived = 0L;
        if (!uploadInProgress) {
            return;
        }
        uploadHandler.abortAndCleanup();
        uploadInProgress = false;
    }

    private void abortAsyncUpload() {
        asyncUploadBytesReceived = 0L;
        if (!asyncUploadInProgress) {
            return;
        }
        asyncUploadInProgress = false;
        asyncUploadChain = asyncUploadChain
                .handle((ignored, throwable) -> null)
                .thenRunAsync(asyncUploadHandler::abortAndCleanup, uploadIoExecutor);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        abortSyncUpload();
        abortAsyncUpload();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        abortSyncUpload();
        abortAsyncUpload();
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error\n", false);
    }

    private static String firstParam(Map<String, List<String>> params, String name) {
        List<String> values = params.get(name);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    private static boolean exceedsLimit(long currentBytes, int nextChunkBytes) {
        if (nextChunkBytes <= 0) {
            return false;
        }
        return currentBytes > MAX_UPLOAD_BYTES - nextChunkBytes;
    }

    private static String buildUploadTooLargeMessage() {
        return "Upload too large, max bytes=" + MAX_UPLOAD_BYTES + "\n";
    }

    private static boolean hasDeclaredLengthTooLarge(HttpRequest request) {
        long contentLength = HttpUtil.getContentLength(request, -1L);
        return contentLength > MAX_UPLOAD_BYTES;
    }

    @FunctionalInterface
    private interface UploadIoTask {
        void run() throws IOException;
    }
}
