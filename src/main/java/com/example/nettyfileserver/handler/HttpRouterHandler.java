package com.example.nettyfileserver.handler;

import com.example.nettyfileserver.util.FilenameValidator;
import com.example.nettyfileserver.util.HttpResponseUtil;
import io.netty.buffer.ByteBuf;
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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Routes HTTP requests and manages upload state for a single connection.
 */
public class HttpRouterHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final Logger LOGGER = Logger.getLogger(HttpRouterHandler.class.getName());

    private static final long MAX_UPLOAD_BYTES = 200L * 1024 * 1024;
    private static final long ASYNC_SLEEP_UNSET = -1L;
    private static final long MAX_ASYNC_SLEEP_MS = 60_000L;
    private static final long DEFAULT_ASYNC_QUEUE_HIGH_WATERMARK_BYTES = 8L * 1024 * 1024;
    private static final long DEFAULT_ASYNC_QUEUE_LOW_WATERMARK_BYTES = 4L * 1024 * 1024;
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
    private boolean syncReadPaused;
    private boolean syncAutoReadManaged;

    private long asyncUploadBytesReceived;
    private long asyncPendingWriteBytes;
    private long asyncPendingWriteTasks;
    private long asyncChunkSleepMs;
    private boolean asyncReadPaused;
    private CompletableFuture<Void> asyncUploadChain;

    private String syncUploadFilename;
    private long syncUploadStartedAtNanos;
    private long syncPersistNanos;
    private long syncChunkCount;

    private String asyncUploadFilename;
    private long asyncUploadStartedAtNanos;
    private long asyncPersistNanos;
    private long asyncChunkCount;
    private long asyncPeakPendingWriteBytes;
    private long asyncPeakPendingWriteTasks;

    public HttpRouterHandler(Path dataDir, ExecutorService uploadIoExecutor) {
        this.uploadHandler = new UploadHandler(dataDir);
        this.asyncUploadHandler = new UploadHandler(dataDir);
        this.downloadHandler = new DownloadHandler(dataDir);
        this.uploadIoExecutor = uploadIoExecutor;
        this.uploadInProgress = false;
        this.asyncUploadInProgress = false;
        this.uploadBytesReceived = 0L;
        this.syncReadPaused = false;
        this.syncAutoReadManaged = false;
        this.asyncUploadBytesReceived = 0L;
        this.asyncPendingWriteBytes = 0L;
        this.asyncPendingWriteTasks = 0L;
        this.asyncChunkSleepMs = ASYNC_SLEEP_UNSET;
        this.asyncReadPaused = false;
        this.asyncUploadChain = CompletableFuture.completedFuture(null);
        clearSyncUploadMetrics();
        clearAsyncUploadMetrics();
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
            if (uploadInProgress) {
                logSyncUploadFailure(ctx, "interrupted_by_new_request", null);
            }
            if (asyncUploadInProgress) {
                logAsyncUploadFailure(ctx, "interrupted_by_new_request", null);
            }
            abortSyncUpload(ctx);
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

        if (HttpMethod.GET.equals(request.method()) && "/debug/leak".equals(path)) {
            handleLeakDebugRequest(ctx, request, query.parameters());
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
            logUploadRejected(ctx, "sync", filename, request, "invalid_filename", null);
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid filename\n", keepAlive);
            return;
        }
        if (hasDeclaredLengthTooLarge(request)) {
            logUploadRejected(ctx, "sync", filename, request, "declared_length_too_large", null);
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
            initSyncUploadMetrics(filename);
            startSyncFlowControl(ctx);
            logUploadStarted(ctx, "sync", filename, request, ASYNC_SLEEP_UNSET);
        } catch (IOException ex) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            uploadBytesReceived = 0L;
            resetSyncFlowControl(ctx);
            clearSyncUploadMetrics();
            logUploadRejected(ctx, "sync", filename, request, "open_target_failed", ex);
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
            logUploadRejected(ctx, "async", filename, request, "invalid_filename", null);
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid filename\n", keepAlive);
            return;
        }
        if (hasDeclaredLengthTooLarge(request)) {
            logUploadRejected(ctx, "async", filename, request, "declared_length_too_large", null);
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
            sleepMs = parseAsyncSleepMs(firstParam(params, "sleepMs"));
        } catch (IllegalArgumentException ex) {
            logUploadRejected(ctx, "async", filename, request, "invalid_sleep_ms", ex);
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, buildInvalidSleepMessage(), keepAlive);
            return;
        }

        if (HttpUtil.is100ContinueExpected(request)) {
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }

        asyncUploadInProgress = true;
        asyncUploadBytesReceived = 0L;
        asyncPendingWriteBytes = 0L;
        asyncPendingWriteTasks = 0L;
        asyncChunkSleepMs = sleepMs;
        asyncReadPaused = false;
        initAsyncUploadMetrics(filename);
        ensureAutoRead(ctx);
        logUploadStarted(ctx, "async", filename, request, sleepMs);

        enqueueAsyncUploadTask(
                ctx,
                "open_target_failed",
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

    private void handleLeakDebugRequest(ChannelHandlerContext ctx, HttpRequest request, Map<String, List<String>> params) {
        boolean keepAlive = HttpUtil.isKeepAlive(request);

        int leakCount;
        int leakBytes;
        boolean triggerGc;
        try {
            leakCount = parsePositiveInt(firstParam(params, "count"), 256, 1, 10_000);
            leakBytes = parsePositiveInt(firstParam(params, "bytes"), 1024, 1, 1024 * 1024);
            String gcParam = firstParam(params, "gc");
            triggerGc = gcParam == null ? true : parseBooleanParam(gcParam);
        } catch (IllegalArgumentException ex) {
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, ex.getMessage() + "\n", keepAlive);
            return;
        }

        for (int i = 0; i < leakCount; i++) {
            ByteBuf leakedBuffer = ctx.alloc().directBuffer(leakBytes);
            leakedBuffer.writeZero(leakBytes);
            leakedBuffer.touch("intentional leak demo idx=" + i + " bytes=" + leakBytes);
        }

        if (triggerGc) {
            System.gc();
        }

        LOGGER.warning(new StringBuilder(192)
                .append("upload_event=leak_demo")
                .append(" remote=").append(quote(remoteAddress(ctx)))
                .append(" leak_count=").append(leakCount)
                .append(" leak_bytes=").append(leakBytes)
                .append(" trigger_gc=").append(triggerGc)
                .toString());

        HttpResponseUtil.sendText(
                ctx,
                HttpResponseStatus.OK,
                "Intentional leak created, count=" + leakCount + ", bytes=" + leakBytes + ", gc=" + triggerGc + "\n",
                keepAlive
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

        if (content instanceof LastHttpContent && !content.content().isReadable()) {
            return;
        }
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Unexpected request body\n", false);
    }

    private void handleSyncUploadContent(ChannelHandlerContext ctx, HttpContent content) {
        int chunkBytes = content.content().readableBytes();
        if (exceedsLimit(uploadBytesReceived, chunkBytes)) {
            logSyncUploadFailure(ctx, "request_entity_too_large", null);
            abortSyncUpload(ctx);
            HttpResponseUtil.sendText(
                    ctx,
                    HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                    buildUploadTooLargeMessage(),
                    false
            );
            return;
        }
        uploadBytesReceived += chunkBytes;
        if (chunkBytes > 0) {
            syncChunkCount += 1;
        }

        long persistStartedAtNanos = System.nanoTime();
        try {
            UploadHandler.UploadResult result = uploadHandler.handleChunk(content);
            syncPersistNanos += System.nanoTime() - persistStartedAtNanos;
            if (result != null) {
                uploadInProgress = false;
                uploadBytesReceived = 0L;
                resetSyncFlowControl(ctx);
                logSyncUploadSuccess(ctx, result);
                clearSyncUploadMetrics();
                String message = "Upload successful: " + result.filename() + ", bytes=" + result.bytesWritten() + "\n";
                HttpResponseUtil.sendText(ctx, HttpResponseStatus.OK, message, result.keepAlive());
                return;
            }
            requestNextSyncChunk(ctx);
        } catch (IllegalStateException ex) {
            syncPersistNanos += System.nanoTime() - persistStartedAtNanos;
            logSyncUploadFailure(ctx, "no_active_upload", ex);
            abortSyncUpload(ctx);
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "No active upload\n", false);
        } catch (IOException ex) {
            syncPersistNanos += System.nanoTime() - persistStartedAtNanos;
            logSyncUploadFailure(ctx, "upload_failed", ex);
            abortSyncUpload(ctx);
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Upload failed\n", false);
        }
    }

    private void handleAsyncUploadContent(ChannelHandlerContext ctx, HttpContent content) {
        int chunkBytes = content.content().readableBytes();
        if (exceedsLimit(asyncUploadBytesReceived, chunkBytes)) {
            logAsyncUploadFailure(ctx, "request_entity_too_large", null);
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
        if (chunkBytes > 0) {
            asyncChunkCount += 1;
        }

        byte[] bytes = new byte[chunkBytes];
        content.content().getBytes(content.content().readerIndex(), bytes);
        boolean lastChunk = content instanceof LastHttpContent;
        long sleepMs = asyncChunkSleepMs;

        if (chunkBytes > 0) {
            asyncPendingWriteBytes += chunkBytes;
            asyncPendingWriteTasks += 1;
            asyncPeakPendingWriteBytes = Math.max(asyncPeakPendingWriteBytes, asyncPendingWriteBytes);
            asyncPeakPendingWriteTasks = Math.max(asyncPeakPendingWriteTasks, asyncPendingWriteTasks);
        }
        applyAsyncBackpressureIfNeeded(ctx);

        enqueueAsyncUploadTask(
                ctx,
                "upload_failed",
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
            String failureReason,
            String errorMessage,
            int queuedBytes,
            UploadIoTask task,
            AsyncUploadSuccess onSuccess
    ) {
        asyncUploadChain = asyncUploadChain.thenCompose(ignored ->
                CompletableFuture
                        .supplyAsync(() -> runTimedUploadTask(task), uploadIoExecutor)
                        .handle((taskResult, throwable) -> {
                            ctx.executor().execute(() -> {
                                onAsyncChunkPersisted(ctx, queuedBytes);
                                if (!asyncUploadInProgress) {
                                    return;
                                }
                                if (taskResult != null && queuedBytes > 0) {
                                    asyncPersistNanos += taskResult.persistNanos();
                                }
                                if (throwable != null) {
                                    failAsyncUpload(ctx, failureReason, errorMessage, throwable);
                                    return;
                                }
                                onSuccess.onSuccess(taskResult.result());
                            });
                            return null;
                        })
        );
    }

    private static AsyncUploadTaskResult runTimedUploadTask(UploadIoTask task) {
        long startedAtNanos = System.nanoTime();
        try {
            return new AsyncUploadTaskResult(task.run(), System.nanoTime() - startedAtNanos);
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
        logAsyncUploadSuccess(ctx, result);
        clearAsyncUploadMetrics();
        String message = "Upload successful: " + result.filename() + ", bytes=" + result.bytesWritten() + "\n";
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.OK, message, result.keepAlive());
    }

    private void failAsyncUpload(ChannelHandlerContext ctx, String failureReason, String errorMessage, Throwable throwable) {
        if (!asyncUploadInProgress) {
            return;
        }
        logAsyncUploadFailure(ctx, failureReason, throwable);
        asyncUploadInProgress = false;
        asyncUploadBytesReceived = 0L;
        asyncUploadChain = CompletableFuture.completedFuture(null);
        resetAsyncFlowControl(ctx);
        clearAsyncUploadMetrics();
        CompletableFuture.runAsync(asyncUploadHandler::abortAndCleanup, uploadIoExecutor);
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, errorMessage, false);
    }

    private void abortSyncUpload(ChannelHandlerContext ctx) {
        uploadBytesReceived = 0L;
        if (!uploadInProgress) {
            resetSyncFlowControl(ctx);
            clearSyncUploadMetrics();
            return;
        }
        uploadHandler.abortAndCleanup();
        uploadInProgress = false;
        resetSyncFlowControl(ctx);
        clearSyncUploadMetrics();
    }

    private void abortAsyncUpload(ChannelHandlerContext ctx) {
        asyncUploadBytesReceived = 0L;
        if (!asyncUploadInProgress) {
            asyncUploadChain = CompletableFuture.completedFuture(null);
            resetAsyncFlowControl(ctx);
            clearAsyncUploadMetrics();
            return;
        }
        asyncUploadInProgress = false;
        asyncUploadChain = asyncUploadChain
                .handle((ignored, throwable) -> null)
                .thenRunAsync(asyncUploadHandler::abortAndCleanup, uploadIoExecutor);
        resetAsyncFlowControl(ctx);
        clearAsyncUploadMetrics();
    }

    private void startSyncFlowControl(ChannelHandlerContext ctx) {
        syncReadPaused = true;
        syncAutoReadManaged = ctx.channel().config().isAutoRead();
        if (syncAutoReadManaged) {
            ctx.channel().config().setAutoRead(false);
        }
        ctx.read();
    }

    private void requestNextSyncChunk(ChannelHandlerContext ctx) {
        if (!uploadInProgress || !syncReadPaused) {
            return;
        }
        ctx.read();
    }

    private void resetSyncFlowControl(ChannelHandlerContext ctx) {
        if (!syncReadPaused) {
            syncAutoReadManaged = false;
            return;
        }
        syncReadPaused = false;
        if (syncAutoReadManaged) {
            syncAutoReadManaged = false;
            ensureAutoRead(ctx);
            return;
        }
        syncAutoReadManaged = false;
    }

    private void applyAsyncBackpressureIfNeeded(ChannelHandlerContext ctx) {
        if (asyncReadPaused || asyncPendingWriteBytes < ASYNC_QUEUE_HIGH_WATERMARK_BYTES) {
            return;
        }
        asyncReadPaused = true;
        ctx.channel().config().setAutoRead(false);
        LOGGER.info(buildAsyncQueueLog(ctx, "backpressure_pause", asyncPendingWriteBytes, asyncPendingWriteTasks));
    }

    private void onAsyncChunkPersisted(ChannelHandlerContext ctx, int queuedBytes) {
        if (queuedBytes > 0) {
            asyncPendingWriteBytes = Math.max(0L, asyncPendingWriteBytes - queuedBytes);
            asyncPendingWriteTasks = Math.max(0L, asyncPendingWriteTasks - 1L);
        }
        if (asyncReadPaused && asyncPendingWriteBytes <= ASYNC_QUEUE_LOW_WATERMARK_BYTES) {
            asyncReadPaused = false;
            ensureAutoRead(ctx);
            LOGGER.info(buildAsyncQueueLog(ctx, "backpressure_resume", asyncPendingWriteBytes, asyncPendingWriteTasks));
        }
    }

    private void resetAsyncFlowControl(ChannelHandlerContext ctx) {
        asyncPendingWriteBytes = 0L;
        asyncPendingWriteTasks = 0L;
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
        if (uploadInProgress) {
            logSyncUploadFailure(ctx, "channel_inactive", null);
        }
        if (asyncUploadInProgress) {
            logAsyncUploadFailure(ctx, "channel_inactive", null);
        }
        abortSyncUpload(ctx);
        abortAsyncUpload(ctx);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (uploadInProgress) {
            logSyncUploadFailure(ctx, "exception_caught", cause);
        }
        if (asyncUploadInProgress) {
            logAsyncUploadFailure(ctx, "exception_caught", cause);
        }
        abortSyncUpload(ctx);
        abortAsyncUpload(ctx);
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error\n", false);
    }

    private void initSyncUploadMetrics(String filename) {
        syncUploadFilename = filename;
        syncUploadStartedAtNanos = System.nanoTime();
        syncPersistNanos = 0L;
        syncChunkCount = 0L;
    }

    private void clearSyncUploadMetrics() {
        syncUploadFilename = null;
        syncUploadStartedAtNanos = 0L;
        syncPersistNanos = 0L;
        syncChunkCount = 0L;
    }

    private void initAsyncUploadMetrics(String filename) {
        asyncUploadFilename = filename;
        asyncUploadStartedAtNanos = System.nanoTime();
        asyncPersistNanos = 0L;
        asyncChunkCount = 0L;
        asyncPeakPendingWriteBytes = 0L;
        asyncPeakPendingWriteTasks = 0L;
    }

    private void clearAsyncUploadMetrics() {
        asyncUploadFilename = null;
        asyncUploadStartedAtNanos = 0L;
        asyncPersistNanos = 0L;
        asyncChunkCount = 0L;
        asyncPeakPendingWriteBytes = 0L;
        asyncPeakPendingWriteTasks = 0L;
    }

    private void logUploadStarted(ChannelHandlerContext ctx, String mode, String filename, HttpRequest request, long sleepMs) {
        StringBuilder message = new StringBuilder(256)
                .append("upload_event=start")
                .append(" mode=").append(mode)
                .append(" remote=").append(quote(remoteAddress(ctx)))
                .append(" filename=").append(quote(filename))
                .append(" declared_bytes=").append(HttpUtil.getContentLength(request, -1L))
                .append(" keep_alive=").append(HttpUtil.isKeepAlive(request));
        if (sleepMs != ASYNC_SLEEP_UNSET) {
            message.append(" sleep_ms=").append(sleepMs);
        }
        LOGGER.info(message.toString());
    }

    private void logUploadRejected(
            ChannelHandlerContext ctx,
            String mode,
            String filename,
            HttpRequest request,
            String reason,
            Throwable throwable
    ) {
        long declaredBytes = request == null ? -1L : HttpUtil.getContentLength(request, -1L);
        String message = new StringBuilder(256)
                .append("upload_event=rejected")
                .append(" mode=").append(mode)
                .append(" remote=").append(quote(remoteAddress(ctx)))
                .append(" filename=").append(quote(filename))
                .append(" declared_bytes=").append(declaredBytes)
                .append(" reason=").append(reason)
                .toString();
        logFailure(message, throwable);
    }

    private void logSyncUploadSuccess(ChannelHandlerContext ctx, UploadHandler.UploadResult result) {
        LOGGER.info(buildUploadSummary(
                ctx,
                "success",
                "sync",
                result.filename(),
                result.bytesWritten(),
                result.bytesWritten(),
                syncUploadStartedAtNanos,
                syncPersistNanos,
                syncChunkCount,
                0L,
                0L,
                0L,
                0L,
                null
        ));
    }

    private void logAsyncUploadSuccess(ChannelHandlerContext ctx, UploadHandler.UploadResult result) {
        LOGGER.info(buildUploadSummary(
                ctx,
                "success",
                "async",
                result.filename(),
                result.bytesWritten(),
                result.bytesWritten(),
                asyncUploadStartedAtNanos,
                asyncPersistNanos,
                asyncChunkCount,
                0L,
                0L,
                asyncPeakPendingWriteBytes,
                asyncPeakPendingWriteTasks,
                null
        ));
    }

    private void logSyncUploadFailure(ChannelHandlerContext ctx, String reason, Throwable throwable) {
        String message = buildUploadSummary(
                ctx,
                "failure",
                "sync",
                syncUploadFilename,
                uploadBytesReceived,
                uploadBytesReceived,
                syncUploadStartedAtNanos,
                syncPersistNanos,
                syncChunkCount,
                0L,
                0L,
                0L,
                0L,
                reason
        );
        logFailure(message, throwable);
    }

    private void logAsyncUploadFailure(ChannelHandlerContext ctx, String reason, Throwable throwable) {
        long persistedBytes = Math.max(0L, asyncUploadBytesReceived - asyncPendingWriteBytes);
        String message = buildUploadSummary(
                ctx,
                "failure",
                "async",
                asyncUploadFilename,
                asyncUploadBytesReceived,
                persistedBytes,
                asyncUploadStartedAtNanos,
                asyncPersistNanos,
                asyncChunkCount,
                asyncPendingWriteBytes,
                asyncPendingWriteTasks,
                asyncPeakPendingWriteBytes,
                asyncPeakPendingWriteTasks,
                reason
        );
        logFailure(message, throwable);
    }

    private String buildUploadSummary(
            ChannelHandlerContext ctx,
            String event,
            String mode,
            String filename,
            long receivedBytes,
            long persistedBytes,
            long startedAtNanos,
            long persistNanos,
            long chunkCount,
            long queuePendingBytes,
            long queuePendingTasks,
            long queuePeakBytes,
            long queuePeakTasks,
            String reason
    ) {
        StringBuilder message = new StringBuilder(320)
                .append("upload_event=").append(event)
                .append(" mode=").append(mode)
                .append(" remote=").append(quote(remoteAddress(ctx)))
                .append(" filename=").append(quote(filename))
                .append(" received_bytes=").append(receivedBytes)
                .append(" persisted_bytes=").append(persistedBytes)
                .append(" total_ms=").append(elapsedMillis(startedAtNanos))
                .append(" persist_ms=").append(nanosToMillis(persistNanos))
                .append(" persist_mib_per_sec=").append(formatMiBPerSec(persistedBytes, persistNanos))
                .append(" chunks=").append(chunkCount);
        if ("async".equals(mode)) {
            message.append(" queue_pending_bytes=").append(queuePendingBytes)
                    .append(" queue_pending_tasks=").append(queuePendingTasks)
                    .append(" queue_peak_bytes=").append(queuePeakBytes)
                    .append(" queue_peak_tasks=").append(queuePeakTasks);
        }
        if (reason != null) {
            message.append(" reason=").append(reason);
        }
        return message.toString();
    }

    private String buildAsyncQueueLog(ChannelHandlerContext ctx, String event, long pendingBytes, long pendingTasks) {
        return new StringBuilder(256)
                .append("upload_event=").append(event)
                .append(" mode=async")
                .append(" remote=").append(quote(remoteAddress(ctx)))
                .append(" filename=").append(quote(asyncUploadFilename))
                .append(" pending_bytes=").append(pendingBytes)
                .append(" pending_tasks=").append(pendingTasks)
                .append(" queue_peak_bytes=").append(asyncPeakPendingWriteBytes)
                .append(" queue_peak_tasks=").append(asyncPeakPendingWriteTasks)
                .append(" high_watermark_bytes=").append(ASYNC_QUEUE_HIGH_WATERMARK_BYTES)
                .append(" low_watermark_bytes=").append(ASYNC_QUEUE_LOW_WATERMARK_BYTES)
                .toString();
    }

    private static void logFailure(String message, Throwable throwable) {
        Throwable rootCause = rootCause(throwable);
        if (rootCause == null) {
            LOGGER.warning(message);
            return;
        }
        LOGGER.log(Level.WARNING, message + " cause=" + quote(rootCause.toString()), rootCause);
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static long elapsedMillis(long startedAtNanos) {
        if (startedAtNanos <= 0L) {
            return 0L;
        }
        return nanosToMillis(System.nanoTime() - startedAtNanos);
    }

    private static long nanosToMillis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, nanos));
    }

    private static String formatMiBPerSec(long bytes, long nanos) {
        if (bytes <= 0L || nanos <= 0L) {
            return "0.00";
        }
        double mibPerSec = bytes * 1_000_000_000.0 / nanos / (1024.0 * 1024.0);
        return String.format(Locale.US, "%.2f", mibPerSec);
    }

    private static String remoteAddress(ChannelHandlerContext ctx) {
        return String.valueOf(ctx.channel().remoteAddress());
    }

    private static String quote(String value) {
        if (value == null || value.isBlank()) {
            return "\"-\"";
        }
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
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

    private static int parsePositiveInt(String value, int defaultValue, int minValue, int maxValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed < minValue || parsed > maxValue) {
                throw new IllegalArgumentException("value out of range: " + value);
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("invalid integer: " + value, ex);
        }
    }

    private static boolean parseBooleanParam(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("invalid boolean: " + value);
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

    private record AsyncUploadTaskResult(UploadHandler.UploadResult result, long persistNanos) {
    }
}
