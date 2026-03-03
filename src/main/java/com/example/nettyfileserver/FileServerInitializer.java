package com.example.nettyfileserver;

import com.example.nettyfileserver.handler.HttpRouterHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

public class FileServerInitializer extends ChannelInitializer<SocketChannel> {
    private final Path dataDir;
    private final ExecutorService uploadIoExecutor;

    public FileServerInitializer(Path dataDir, ExecutorService uploadIoExecutor) {
        this.dataDir = dataDir;
        this.uploadIoExecutor = uploadIoExecutor;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        // Decode/encode raw bytes as HTTP objects.
        pipeline.addLast(new HttpServerCodec());
        // Required for streaming file downloads with HttpChunkedInput/ChunkedFile.
        pipeline.addLast(new ChunkedWriteHandler());
        // Route upload/download endpoints and drive streaming I/O.
        pipeline.addLast(new HttpRouterHandler(dataDir, uploadIoExecutor));
    }
}
