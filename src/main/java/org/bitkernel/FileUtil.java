package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Slf4j
public class FileUtil {
    public static void deleteFile(@NotNull String filePath) {
        if (!exist(filePath)) {
            logger.error("File [{}] not exist", filePath);
            return;
        }
        File file = new File(filePath);
        if (file.delete()) {
            logger.debug("Delete file [{}] success", filePath);
        } else {
            logger.debug("Delete file [{}] failed", filePath);
        }
    }

    /**
     * Create a folder that does nothing if it exists,
     * or recursively create if it doesn't.
     *
     * @param dir directory path
     * @return is the creation successful
     */
    public static boolean createFolder(@NotNull String dir) {
        File file = new File(dir);
        if (!file.exists()) {
            if (!file.mkdirs()) {
                logger.error("create folder error: {}", dir);
                return false;
            }
            logger.debug("Create folder success: {}", dir);
        } else {
            logger.debug("Folder already exist: {}", dir);
        }
        return true;
    }

    /**
     * @param filePath file path
     * @return exist or not
     */
    public static boolean exist(@NotNull String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public static void write(@NotNull String filePath, @NotNull String content) {
        try {
            FileWriter writer = new FileWriter(filePath);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            logger.error("Failed to write content to {}", filePath);
        }
    }
}
