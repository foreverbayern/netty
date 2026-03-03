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

public class UploadHandler {
    // Optional per-chunk delay for sync upload path (/upload).
    private static final long SLOW_MS = nonNegative(Long.getLong("slow.ms", 0L));
    // Optional per-chunk delay for async upload path (/upload-async).
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

        if (httpContent instanceof LastHttpContent) {
            UploadResult result = new UploadResult(currentPath.getFileName().toString(), bytesWritten, keepAlive);
            closeChannelQuietly();
            return result;
        }

        return null;
    }

    public UploadResult handleChunkBytes(byte[] bytes, boolean lastChunk) throws IOException {
        // Preserve existing behavior when request-level override is not provided.
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
        // Request param wins; fallback to JVM property-based default.
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
                // Ignore cleanup failures during abort.
            }
        }
    }

    public void closeChannelQuietly() {
        if (currentChannel != null) {
            try {
                currentChannel.close();
            } catch (IOException ignored) {
                // Ignore close failures.
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
            throw new IOException("Interrupted while applying upload delay", ex);
        }
    }

    private static long nonNegative(long value) {
        return Math.max(0L, value);
    }

    public record UploadResult(String filename, long bytesWritten, boolean keepAlive) {
    }
}
