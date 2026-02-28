package com.example.nettyfileserver.handler;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class UploadHandler {
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
        while (content.isReadable()) {
            int written = content.readBytes(currentChannel, content.readableBytes());
            if (written <= 0) {
                throw new IOException("Failed to write upload chunk");
            }
            bytesWritten += written;
        }

        if (httpContent instanceof LastHttpContent) {
            UploadResult result = new UploadResult(currentPath.getFileName().toString(), bytesWritten, keepAlive);
            closeChannelQuietly();
            return result;
        }

        return null;
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

    public record UploadResult(String filename, long bytesWritten, boolean keepAlive) {
    }
}
