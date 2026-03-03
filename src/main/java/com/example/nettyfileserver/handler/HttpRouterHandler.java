package com.example.nettyfileserver.handler;

import com.example.nettyfileserver.util.FilenameValidator;
import com.example.nettyfileserver.util.HttpResponseUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

public class HttpRouterHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final long MAX_UPLOAD_BYTES = 200L * 1024 * 1024;
    private static final long ASYNC_SLEEP_UNSET = -1L;
    private static final long MAX_ASYNC_SLEEP_MS = 60_000L;
    private static final long DEFAULT_ASYNC_QUEUE_HIGH_WATERMARK_BYTES = 8L * 1024 * 1024;
    private static final long DEFAULT_ASYNC_QUEUE_LOW_WATERMARK_BYTES = 4L * 1024 * 1024;
    // Pending async write bytes >= HIGH: pause socket read; <= LOW: resume read.
    private static final long ASYNC_QUEUE_HIGH_WATERMARK_BYTES =
            Math.max(64L * 1024L, Long.getLong("upload.async.queue.high.bytes", DEFAULT_ASYNC_QUEUE_HIGH_WATERMARK_BYTES));
    private static final long ASYNC_QUEUE_LOW_WATERMARK_BYTES =
            Math.min(
                    ASYNC_QUEUE_HIGH_WATERMARK_BYTES,
                    Math.max(32L * 1024L, Long.getLong("upload.async.queue.low.bytes", DEFAULT_ASYNC_QUEUE_LOW_WATERMARK_BYTES))
            );

    private final UploadHandler uploadHandler;
    private final UploadHandler asyncUploadHandler;
    private final DownloadHandler downloadHandler;
    private final ExecutorService uploadIoExecutor;

    private boolean uploadInProgress;
    private boolean asyncUploadInProgress;
    private long uploadBytesReceived;
    private long asyncUploadBytesReceived;
    private long asyncPendingWriteBytes;
    private long asyncChunkSleepMs;
    private boolean asyncReadPaused;
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
        this.asyncPendingWriteBytes = 0L;
        this.asyncChunkSleepMs = ASYNC_SLEEP_UNSET;
        this.asyncReadPaused = false;
        this.asyncUploadChain = CompletableFuture.completedFuture(null);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
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
            abortAsyncUpload(ctx);
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

        long sleepMs;
        try {
            // sleepMs is only for async upload simulation; sync path still uses slow.ms.
            sleepMs = parseAsyncSleepMs(firstParam(params, "sleepMs"));
        } catch (IllegalArgumentException ex) {
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, buildInvalidSleepMessage(), keepAlive);
            return;
        }

        if (HttpUtil.is100ContinueExpected(request)) {
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }

        asyncUploadInProgress = true;
        asyncUploadBytesReceived = 0L;
        asyncPendingWriteBytes = 0L;
        asyncChunkSleepMs = sleepMs;
        asyncReadPaused = false;
        ensureAutoRead(ctx);

        enqueueAsyncUploadTask(
                ctx,
                "Failed to open upload target\n",
                0,
                () -> {
                    asyncUploadHandler.beginUpload(filename, keepAlive);
                    return null;
                },
                result -> {
                }
        );
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
            abortAsyncUpload(ctx);
            HttpResponseUtil.sendText(
                    ctx,
                    HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                    buildUploadTooLargeMessage(),
                    false
            );
            return;
        }
        asyncUploadBytesReceived += chunkBytes;

        // HttpContent will be auto-released after channelRead0 returns, so copy bytes now.
        byte[] bytes = new byte[chunkBytes];
        content.content().getBytes(content.content().readerIndex(), bytes);
        boolean lastChunk = content instanceof LastHttpContent;
        long sleepMs = asyncChunkSleepMs;

        asyncPendingWriteBytes += chunkBytes;
        // Apply inbound backpressure before queueing another async disk write task.
        applyAsyncBackpressureIfNeeded(ctx);

        enqueueAsyncUploadTask(
                ctx,
                "Upload failed\n",
                chunkBytes,
                () -> asyncUploadHandler.handleChunkBytes(bytes, lastChunk, sleepMs),
                result -> {
                    if (result != null) {
                        completeAsyncUpload(ctx, result);
                    }
                }
        );
    }

    private void enqueueAsyncUploadTask(
            ChannelHandlerContext ctx,
            String errorMessage,
            int queuedBytes,
            UploadIoTask task,
            AsyncUploadSuccess onSuccess
    ) {
        // Chain tasks to preserve write order; always hop completion back to EventLoop.
        asyncUploadChain = asyncUploadChain.thenCompose(ignored ->
                CompletableFuture
                        .supplyAsync(() -> runUploadTask(task), uploadIoExecutor)
                        .handle((result, throwable) -> {
                            ctx.executor().execute(() -> {
                                onAsyncChunkPersisted(ctx, queuedBytes);
                                if (throwable != null) {
                                    failAsyncUpload(ctx, errorMessage);
                                    return;
                                }
                                if (!asyncUploadInProgress) {
                                    return;
                                }
                                onSuccess.onSuccess(result);
                            });
                            return null;
                        })
        );
    }

    private static UploadHandler.UploadResult runUploadTask(UploadIoTask task) {
        try {
            return task.run();
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
        resetAsyncFlowControl(ctx);
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
        resetAsyncFlowControl(ctx);
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

    private void abortAsyncUpload(ChannelHandlerContext ctx) {
        asyncUploadBytesReceived = 0L;
        if (!asyncUploadInProgress) {
            asyncUploadChain = CompletableFuture.completedFuture(null);
            resetAsyncFlowControl(ctx);
            return;
        }
        asyncUploadInProgress = false;
        asyncUploadChain = asyncUploadChain
                .handle((ignored, throwable) -> null)
                .thenRunAsync(asyncUploadHandler::abortAndCleanup, uploadIoExecutor);
        resetAsyncFlowControl(ctx);
    }

    private void applyAsyncBackpressureIfNeeded(ChannelHandlerContext ctx) {
        if (asyncReadPaused || asyncPendingWriteBytes < ASYNC_QUEUE_HIGH_WATERMARK_BYTES) {
            return;
        }
        // Too much pending disk work: stop reading more bytes from the socket.
        asyncReadPaused = true;
        ctx.channel().config().setAutoRead(false);
    }

    private void onAsyncChunkPersisted(ChannelHandlerContext ctx, int queuedBytes) {
        if (queuedBytes > 0) {
            asyncPendingWriteBytes = Math.max(0L, asyncPendingWriteBytes - queuedBytes);
        }
        if (asyncReadPaused && asyncPendingWriteBytes <= ASYNC_QUEUE_LOW_WATERMARK_BYTES) {
            // Queue drained enough: re-enable socket reads.
            asyncReadPaused = false;
            ensureAutoRead(ctx);
        }
    }

    private void resetAsyncFlowControl(ChannelHandlerContext ctx) {
        asyncPendingWriteBytes = 0L;
        asyncChunkSleepMs = ASYNC_SLEEP_UNSET;
        if (asyncReadPaused) {
            asyncReadPaused = false;
            ensureAutoRead(ctx);
        }
    }

    private static void ensureAutoRead(ChannelHandlerContext ctx) {
        if (ctx.channel().config().isAutoRead()) {
            return;
        }
        ctx.channel().config().setAutoRead(true);
        ctx.read();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        abortSyncUpload();
        abortAsyncUpload(ctx);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        abortSyncUpload();
        abortAsyncUpload(ctx);
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

    private static long parseAsyncSleepMs(String sleepParam) {
        if (sleepParam == null || sleepParam.isBlank()) {
            return ASYNC_SLEEP_UNSET;
        }
        try {
            long sleepMs = Long.parseLong(sleepParam);
            if (sleepMs < 0L || sleepMs > MAX_ASYNC_SLEEP_MS) {
                throw new IllegalArgumentException("sleepMs out of range");
            }
            return sleepMs;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("sleepMs is not a number", ex);
        }
    }

    private static String buildInvalidSleepMessage() {
        return "Invalid sleepMs, expected 0-" + MAX_ASYNC_SLEEP_MS + "\n";
    }

    @FunctionalInterface
    private interface UploadIoTask {
        UploadHandler.UploadResult run() throws IOException;
    }

    @FunctionalInterface
    private interface AsyncUploadSuccess {
        void onSuccess(UploadHandler.UploadResult result);
    }
}
