package com.example.nettyfileserver;

import com.example.nettyfileserver.handler.HttpRouterHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

/**
 * 每个新连接的 pipeline 初始化器。
 * 按顺序安装 HTTP 编解码、分块写出能力和业务路由处理器。
 */
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
        // 将字节流编解码为 HttpRequest/HttpContent/HttpResponse。
        pipeline.addLast(new HttpServerCodec());
        // 支持 ChunkedFile / HttpChunkedInput 的大文件流式回写。
        pipeline.addLast(new ChunkedWriteHandler());
        // 按路径分发上传/下载请求，并驱动流式 I/O 逻辑。
        pipeline.addLast(new HttpRouterHandler(dataDir, uploadIoExecutor));
    }
}
