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

/**
 * HTTP 路由入口。
 * 在单个连接内管理上传/下载状态，并协调异步上传的排队与背压。
 */
public class HttpRouterHandler extends SimpleChannelInboundHandler<HttpObject> {
    // 单次上传最大允许字节数（同步与异步路径共享）。
    private static final long MAX_UPLOAD_BYTES = 200L * 1024 * 1024;
    // 异步上传未指定 sleepMs 时的哨兵值。
    private static final long ASYNC_SLEEP_UNSET = -1L;
    // 接口参数 sleepMs 的上限，避免人为配置过大阻塞。
    private static final long MAX_ASYNC_SLEEP_MS = 60_000L;
    private static final long DEFAULT_ASYNC_QUEUE_HIGH_WATERMARK_BYTES = 8L * 1024 * 1024;
    private static final long DEFAULT_ASYNC_QUEUE_LOW_WATERMARK_BYTES = 4L * 1024 * 1024;
    // 待落盘字节 >= 高水位时暂停读；回落到低水位后恢复读。
    private static final long ASYNC_QUEUE_HIGH_WATERMARK_BYTES =
            Math.max(64L * 1024L, Long.getLong("upload.async.queue.high.bytes", DEFAULT_ASYNC_QUEUE_HIGH_WATERMARK_BYTES));
    private static final long ASYNC_QUEUE_LOW_WATERMARK_BYTES =
            Math.min(
                    ASYNC_QUEUE_HIGH_WATERMARK_BYTES,
                    Math.max(32L * 1024L, Long.getLong("upload.async.queue.low.bytes", DEFAULT_ASYNC_QUEUE_LOW_WATERMARK_BYTES))
            );

    // 同步上传会话状态。
    private final UploadHandler uploadHandler;
    // 异步上传会话状态（与同步路径隔离，便于独立清理）。
    private final UploadHandler asyncUploadHandler;
    // 下载处理器。
    private final DownloadHandler downloadHandler;
    // 异步落盘线程池。
    private final ExecutorService uploadIoExecutor;

    // 同步上传是否进行中。
    private boolean uploadInProgress;
    // 异步上传是否进行中。
    private boolean asyncUploadInProgress;
    // 同步上传累计接收字节，用于限流判断。
    private long uploadBytesReceived;
    // 同步上传是否已切到手动 read 模式。
    private boolean syncReadPaused;
    // 是否由本 handler 关闭了 autoRead（用于恢复时判定）。
    private boolean syncAutoReadManaged;
    // 异步上传累计接收字节，用于限流判断。
    private long asyncUploadBytesReceived;
    // 已排队但未持久化的字节数，用于入站背压。
    private long asyncPendingWriteBytes;
    // 当前异步上传请求的每 chunk 延迟参数。
    private long asyncChunkSleepMs;
    // 是否已关闭 autoRead 以施加背压。
    private boolean asyncReadPaused;
    // 异步任务链，保证 chunk 的落盘顺序。
    private CompletableFuture<Void> asyncUploadChain;

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
        this.asyncChunkSleepMs = ASYNC_SLEEP_UNSET;
        this.asyncReadPaused = false;
        this.asyncUploadChain = CompletableFuture.completedFuture(null);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        // 一个 HTTP 请求会拆成 HttpRequest + 若干 HttpContent，这里分别处理。
        if (msg instanceof HttpRequest request) {
            handleRequest(ctx, request);
        }

        if (msg instanceof HttpContent content) {
            handleContent(ctx, content);
        }
    }

    private void handleRequest(ChannelHandlerContext ctx, HttpRequest request) {
        // 简化语义：同一连接上不允许并发进行两次上传。
        if (uploadInProgress || asyncUploadInProgress) {
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

        // 上传（同步落盘）。
        if (HttpMethod.POST.equals(request.method()) && "/upload".equals(path)) {
            handleUploadRequest(ctx, request, query.parameters());
            return;
        }

        // 上传（异步线程池落盘）。
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
            // 客户端带 Expect: 100-continue 时先放行再接收 body。
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }

        try {
            uploadHandler.beginUpload(filename, keepAlive);
            uploadInProgress = true;
            uploadBytesReceived = 0L;
            startSyncFlowControl(ctx);
        } catch (IOException ex) {
            uploadHandler.abortAndCleanup();
            uploadInProgress = false;
            uploadBytesReceived = 0L;
            resetSyncFlowControl(ctx);
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to open upload target\n", false);
        }
    }

    private void handleDownloadRequest(ChannelHandlerContext ctx, HttpRequest request, Map<String, List<String>> params) {
        String filename = firstParam(params, "filename");
        if (!FilenameValidator.isSafeFilename(filename)) {
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid filename\n", HttpUtil.isKeepAlive(request));
            return;
        }

        // 具体文件读写由 DownloadHandler 负责。
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
            // sleepMs 仅作用于 /upload-async，用于模拟异步落盘延迟。
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
        // 保证开始异步上传前连接处于可读状态。
        ensureAutoRead(ctx);

        // 先异步创建目标文件，后续 chunk 才能入队写入。
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
        // 根据当前会话状态，把 body chunk 分发到对应上传路径。
        if (uploadInProgress) {
            handleSyncUploadContent(ctx, content);
            return;
        }

        if (asyncUploadInProgress) {
            handleAsyncUploadContent(ctx, content);
            return;
        }

        // 普通请求尾部常附带空 LastHttpContent，此处直接忽略。
        if (content instanceof LastHttpContent && !content.content().isReadable()) {
            return;
        }
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "Unexpected request body\n", false);
    }

    private void handleSyncUploadContent(ChannelHandlerContext ctx, HttpContent content) {
        int chunkBytes = content.content().readableBytes();
        if (exceedsLimit(uploadBytesReceived, chunkBytes)) {
            // 一旦超限立即中止并删除临时文件。
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

        try {
            UploadHandler.UploadResult result = uploadHandler.handleChunk(content);
            if (result != null) {
                uploadInProgress = false;
                uploadBytesReceived = 0L;
                resetSyncFlowControl(ctx);
                String message = "Upload successful: " + result.filename() + ", bytes=" + result.bytesWritten() + "\n";
                HttpResponseUtil.sendText(ctx, HttpResponseStatus.OK, message, result.keepAlive());
                return;
            }
            requestNextSyncChunk(ctx);
        } catch (IllegalStateException ex) {
            abortSyncUpload(ctx);
            HttpResponseUtil.sendText(ctx, HttpResponseStatus.BAD_REQUEST, "No active upload\n", false);
        } catch (IOException ex) {
            abortSyncUpload(ctx);
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

        // channelRead0 返回后 HttpContent 会被释放，需先复制出原始字节。
        byte[] bytes = new byte[chunkBytes];
        content.content().getBytes(content.content().readerIndex(), bytes);
        boolean lastChunk = content instanceof LastHttpContent;
        long sleepMs = asyncChunkSleepMs;

        asyncPendingWriteBytes += chunkBytes;
        // 入队前先评估背压，避免继续从 socket 读取过多数据。
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
        // 用 CompletableFuture 串行拼接任务，保证 chunk 按接收顺序落盘。
        // 回调切回 EventLoop 执行，避免并发修改 handler 状态。
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
            // 统一转换为 CompletionException，便于上游在异步链里处理。
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
        // 清理文件在 I/O 线程执行，避免阻塞 EventLoop。
        CompletableFuture.runAsync(asyncUploadHandler::abortAndCleanup, uploadIoExecutor);
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, errorMessage, false);
    }

    private void abortSyncUpload(ChannelHandlerContext ctx) {
        uploadBytesReceived = 0L;
        if (!uploadInProgress) {
            resetSyncFlowControl(ctx);
            return;
        }
        uploadHandler.abortAndCleanup();
        uploadInProgress = false;
        resetSyncFlowControl(ctx);
    }

    private void abortAsyncUpload(ChannelHandlerContext ctx) {
        asyncUploadBytesReceived = 0L;
        if (!asyncUploadInProgress) {
            asyncUploadChain = CompletableFuture.completedFuture(null);
            resetAsyncFlowControl(ctx);
            return;
        }
        // 等待已入队任务链结束后再清理，避免与写文件并发冲突。
        asyncUploadInProgress = false;
        asyncUploadChain = asyncUploadChain
                .handle((ignored, throwable) -> null)
                .thenRunAsync(asyncUploadHandler::abortAndCleanup, uploadIoExecutor);
        resetAsyncFlowControl(ctx);
    }

    private void startSyncFlowControl(ChannelHandlerContext ctx) {
        // 同步上传切到“手动 read”模式：每处理完一个 chunk 再拉取下一个。
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
        // 待写入积压过高，临时关闭 autoRead，阻止继续吸收网络数据。
        asyncReadPaused = true;
        ctx.channel().config().setAutoRead(false);
    }

    private void onAsyncChunkPersisted(ChannelHandlerContext ctx, int queuedBytes) {
        if (queuedBytes > 0) {
            asyncPendingWriteBytes = Math.max(0L, asyncPendingWriteBytes - queuedBytes);
        }
        if (asyncReadPaused && asyncPendingWriteBytes <= ASYNC_QUEUE_LOW_WATERMARK_BYTES) {
            // 积压回落到低水位后恢复读取，形成高低水位滞回控制。
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
        // 重新打开 autoRead 后主动 read 一次，尽快恢复消费。
        ctx.channel().config().setAutoRead(true);
        ctx.read();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        abortSyncUpload(ctx);
        abortAsyncUpload(ctx);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 统一兜底：发生异常后终止上传状态并返回 500。
        abortSyncUpload(ctx);
        abortAsyncUpload(ctx);
        HttpResponseUtil.sendText(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error\n", false);
    }

    private static String firstParam(Map<String, List<String>> params, String name) {
        // 仅取首个同名参数，避免多值参数带来的歧义。
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
        // 若请求头已声明 content-length，先做早期拒绝。
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
            // 统一转成 IllegalArgumentException 供上层返回 400。
            throw new IllegalArgumentException("sleepMs is not a number", ex);
        }
    }

    private static String buildInvalidSleepMessage() {
        return "Invalid sleepMs, expected 0-" + MAX_ASYNC_SLEEP_MS + "\n";
    }

    @FunctionalInterface
    private interface UploadIoTask {
        // 在上传 I/O 线程中执行的任务抽象。
        UploadHandler.UploadResult run() throws IOException;
    }

    @FunctionalInterface
    private interface AsyncUploadSuccess {
        // 异步任务成功后在 EventLoop 中执行的回调。
        void onSuccess(UploadHandler.UploadResult result);
    }
}
