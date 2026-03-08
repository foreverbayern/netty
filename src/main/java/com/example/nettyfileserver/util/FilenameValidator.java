package com.example.nettyfileserver.util;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;

/**
 * 文件名安全校验，阻止目录穿越和绝对路径访问。
 */
public final class FilenameValidator {
    private FilenameValidator() {
    }

    public static boolean isSafeFilename(String filename) {
        // 基础空值与保留字符过滤。
        if (filename == null || filename.isBlank()) {
            return false;
        }
        if (filename.contains("..") || filename.contains("/") || filename.contains("\\") || filename.contains(":")) {
            return false;
        }

        try {
            Path path = Path.of(filename);
            // 仅允许单段文件名，不允许子目录、盘符或绝对路径。
            return !path.isAbsolute()
                    && path.getNameCount() == 1
                    && filename.equals(path.getFileName().toString());
        } catch (InvalidPathException ex) {
            return false;
        }
    }
}
