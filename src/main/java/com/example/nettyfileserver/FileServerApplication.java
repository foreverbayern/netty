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

/**
 * Netty 文件服务器启动入口。
 * 负责初始化网络线程与上传 I/O 线程池，并在进程退出时完成资源回收。
 */
public final class FileServerApplication {
    // 默认监听端口，可通过命令行参数覆盖。
    private static final int DEFAULT_PORT = 8080;
    // 业务演示场景下固定为单 worker，便于观察 EventLoop 行为。
    private static final int WORKER_THREADS = 1;
    // 异步上传线程池默认按 CPU 核数创建。
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
        // 支持两类参数：纯数字端口，或 -Dkey=value 形式的系统属性。
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
        // 仅处理合法的 -Dkey=value，非法参数直接忽略，避免启动失败。
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
        // 为线程池命名，便于通过日志和 jstack 定位异步上传任务。
        AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static void shutdownUploadExecutor(ExecutorService executor) throws InterruptedException {
        // 先优雅关闭，超时后再强制中断，避免进程悬挂。
        executor.shutdown();
        if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
            return;
        }
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
