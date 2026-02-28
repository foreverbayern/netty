package com.example.nettyfileserver;

import com.example.nettyfileserver.handler.HttpRouterHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.nio.file.Path;

public class FileServerInitializer extends ChannelInitializer<SocketChannel> {
    private final Path dataDir;

    public FileServerInitializer(Path dataDir) {
        this.dataDir = dataDir;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(new HttpRouterHandler(dataDir));
    }
}
