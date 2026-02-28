package com.example.nettyfileserver.util;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;

public final class FilenameValidator {
    private FilenameValidator() {
    }

    public static boolean isSafeFilename(String filename) {
        if (filename == null || filename.isBlank()) {
            return false;
        }
        if (filename.contains("..") || filename.contains("/") || filename.contains("\\") || filename.contains(":")) {
            return false;
        }

        try {
            Path path = Path.of(filename);
            return !path.isAbsolute()
                    && path.getNameCount() == 1
                    && filename.equals(path.getFileName().toString());
        } catch (InvalidPathException ex) {
            return false;
        }
    }
}
