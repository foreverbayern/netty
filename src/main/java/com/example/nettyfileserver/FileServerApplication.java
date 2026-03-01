package com.example.nettyfileserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class FileServerApplication {
    private static final int DEFAULT_PORT = 8080;
    private static final int WORKER_THREADS = 1;
    private static final int DEFAULT_UPLOAD_IO_THREADS = Math.max(1, Runtime.getRuntime().availableProcessors());

    private FileServerApplication() {
    }

    public static void main(String[] args) throws Exception {
        int port = parsePort(args);
        Path dataDir = Path.of("data");
        Files.createDirectories(dataDir);
        int uploadIoThreads = Integer.getInteger("upload.io.threads", DEFAULT_UPLOAD_IO_THREADS);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        ExecutorService uploadIoExecutor = Executors.newFixedThreadPool(
                Math.max(1, uploadIoThreads),
                namedThreadFactory("upload-io")
        );

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new FileServerInitializer(dataDir, uploadIoExecutor));

            Channel channel = bootstrap.bind(port).sync().channel();
            System.out.println("Netty file server started on port " + port);
            System.out.println("Data directory: " + dataDir.toAbsolutePath());
            System.out.println("Upload IO pool threads: " + Math.max(1, uploadIoThreads));
            channel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
            shutdownUploadExecutor(uploadIoExecutor);
        }
    }

    private static int parsePort(String[] args) {
        for (String arg : args) {
            if (arg.isBlank()) {
                continue;
            }
            if (arg.startsWith("-D")) {
                applySystemPropertyArg(arg);
                continue;
            }
            try {
                return Integer.parseInt(arg);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Invalid port: " + arg, ex);
            }
        }
        return DEFAULT_PORT;
    }

    private static void applySystemPropertyArg(String arg) {
        int separator = arg.indexOf('=');
        if (separator <= 2 || separator == arg.length() - 1) {
            return;
        }
        String key = arg.substring(2, separator).trim();
        if (key.isEmpty()) {
            return;
        }
        String value = arg.substring(separator + 1);
        System.setProperty(key, value);
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static void shutdownUploadExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdown();
        if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
            return;
        }
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
