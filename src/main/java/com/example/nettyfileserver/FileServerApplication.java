package com.example.nettyfileserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.nio.file.Files;
import java.nio.file.Path;

public final class FileServerApplication {
    private static final int DEFAULT_PORT = 8080;

    private FileServerApplication() {
    }

    public static void main(String[] args) throws Exception {
        int port = parsePort(args);
        Path dataDir = Path.of("data");
        Files.createDirectories(dataDir);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new FileServerInitializer(dataDir));

            Channel channel = bootstrap.bind(port).sync().channel();
            System.out.println("Netty file server started on port " + port);
            System.out.println("Data directory: " + dataDir.toAbsolutePath());
            channel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
        }
    }

    private static int parsePort(String[] args) {
        if (args == null || args.length == 0) {
            return DEFAULT_PORT;
        }
        try {
            return Integer.parseInt(args[0]);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid port: " + args[0], ex);
        }
    }
}
