package com.example.nettyfileserver.handler;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 单文件上传写入器。
 * 每次 beginUpload 开启一次会话，后续 chunk 持续追加写入，最后由 LastHttpContent 收尾。
 */
public class UploadHandler {
    // /upload 路径每个 chunk 的可选延迟，用于模拟慢磁盘。
    private static final long SLOW_MS = nonNegative(Long.getLong("slow.ms", 0L));
    // /upload-async 默认延迟；若未单请求覆盖，则沿用该值。
    private static final long SLOW_ASYNC_MS = nonNegative(Long.getLong("slow.async.ms", SLOW_MS));

    private final Path dataDir;

    private FileChannel currentChannel;
    private Path currentPath;
    private long bytesWritten;
    private boolean keepAlive;

    public UploadHandler(Path dataDir) {
        this.dataDir = dataDir;
    }

    public void beginUpload(String filename, boolean keepAlive) throws IOException {
        // 启动新会话前先清理旧状态，避免句柄泄漏。
        closeChannelQuietly();
        this.currentPath = dataDir.resolve(filename).normalize();
        this.currentChannel = FileChannel.open(
                currentPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        );
        this.bytesWritten = 0L;
        this.keepAlive = keepAlive;
    }

    public UploadResult handleChunk(HttpContent httpContent) throws IOException {
        if (currentChannel == null) {
            throw new IllegalStateException("No active upload");
        }

        ByteBuf content = httpContent.content();
        long bytesBeforeChunk = bytesWritten;
        while (content.isReadable()) {
            int written = content.readBytes(currentChannel, content.readableBytes());
            if (written <= 0) {
                throw new IOException("Failed to write upload chunk");
            }
            bytesWritten += written;
        }
        if (bytesWritten > bytesBeforeChunk) {
            maybeSleepAfterChunk(SLOW_MS);
        }

        // 收到最后一块后关闭通道，并返回响应所需元信息。
        if (httpContent instanceof LastHttpContent) {
            UploadResult result = new UploadResult(currentPath.getFileName().toString(), bytesWritten, keepAlive);
            closeChannelQuietly();
            return result;
        }

        return null;
    }

    public UploadResult handleChunkBytes(byte[] bytes, boolean lastChunk) throws IOException {
        // 未传请求级覆盖值时，保持原有 JVM 参数行为。
        return handleChunkBytes(bytes, lastChunk, -1L);
    }

    public UploadResult handleChunkBytes(byte[] bytes, boolean lastChunk, long asyncSleepMsOverride) throws IOException {
        if (currentChannel == null) {
            throw new IllegalStateException("No active upload");
        }

        if (bytes.length > 0) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            while (byteBuffer.hasRemaining()) {
                int written = currentChannel.write(byteBuffer);
                if (written <= 0) {
                    throw new IOException("Failed to write upload chunk");
                }
                bytesWritten += written;
            }
            maybeSleepAfterChunk(resolveAsyncSleepMs(asyncSleepMsOverride));
        }

        if (lastChunk) {
            UploadResult result = new UploadResult(currentPath.getFileName().toString(), bytesWritten, keepAlive);
            closeChannelQuietly();
            return result;
        }

        return null;
    }

    private static long resolveAsyncSleepMs(long asyncSleepMsOverride) {
        // 请求参数优先；未设置时回退到 JVM 属性。
        if (asyncSleepMsOverride >= 0L) {
            return asyncSleepMsOverride;
        }
        return SLOW_ASYNC_MS;
    }

    public boolean isUploading() {
        return currentChannel != null;
    }

    public void abortAndCleanup() {
        Path pathToDelete = currentPath;
        closeChannelQuietly();
        if (pathToDelete != null) {
            try {
                Files.deleteIfExists(pathToDelete);
            } catch (IOException ignored) {
                // 中止路径下的清理失败不影响连接语义，记录即可。
            }
        }
    }

    public void closeChannelQuietly() {
        if (currentChannel != null) {
            try {
                currentChannel.close();
            } catch (IOException ignored) {
                // 关闭失败不再重试，避免阻塞 EventLoop。
            }
            currentChannel = null;
        }
        currentPath = null;
        bytesWritten = 0L;
        keepAlive = false;
    }

    private static void maybeSleepAfterChunk(long sleepMs) throws IOException {
        if (sleepMs <= 0L) {
            return;
        }
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // 将中断语义上抛给调用方，避免静默吞掉中断信号。
            throw new IOException("Interrupted while applying upload delay", ex);
        }
    }

    private static long nonNegative(long value) {
        return Math.max(0L, value);
    }

    public record UploadResult(String filename, long bytesWritten, boolean keepAlive) {
    }
}
