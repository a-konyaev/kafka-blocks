package kafkablocks.utils;

import org.apache.commons.io.IOUtils;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;

/**
 * Работа с файловой системой
 */
public final class FileUtils {
    private FileUtils() {
    }

    /**
     * Проверяем, что папка для логов существует и доступна для записи.
     * Если папки нет, то пытаемся создать ее.
     */
    public static void ensureDirectoryExists(String path) {
        File dir = new File(path);

        if (dir.exists()) {
            if (!dir.canWrite())
                throw new RuntimeException("No write permissions for directory: " + path);

            return;
        }

        try {
            dir.mkdirs();
        } catch (SecurityException e) {
            throw new RuntimeException("Error while creating the directory or its parent directories: " + path, e);
        }
    }

    public static String getFilePath(String dirPath, String filename, String extension) {
        return Paths.get(dirPath, String.format("%s.%s", filename, extension)).toString();
    }

    /**
     * Загружает файл из ресурсов и возвращает его содержимое в виде строки
     *
     * @param resFilePath путь к файлу в ресурсах.
     *                    если файл лежит в корне ресурсной папки, то путь должен быть например таким "/my-res.txt"
     */
    public static String readFileFromResource(String resFilePath) {
        try {
            try (InputStream inputStream = FileUtils.class.getResourceAsStream(resFilePath)) {
                return IOUtils.toString(inputStream, Charset.defaultCharset());
            }
        } catch (Throwable e) {
            throw new RuntimeException(
                    String.format("Reading file '%s' from resources failed: %s", resFilePath, e.getMessage()),
                    e);
        }
    }

    private static final MimetypesFileTypeMap mimetypesFileTypeMap = new MimetypesFileTypeMap();
    static {
        mimetypesFileTypeMap.addMimeTypes("model/gltf-binary glb");
        mimetypesFileTypeMap.addMimeTypes("image/png png");
        mimetypesFileTypeMap.addMimeTypes("video/mp4 mp4");
        mimetypesFileTypeMap.addMimeTypes("video/mp4 mpg4");
        mimetypesFileTypeMap.addMimeTypes("application/zip zip");
        mimetypesFileTypeMap.addMimeTypes("application/gzip gzip");
    }

    public static String getContentTypeByExtension(String fileName) {
        return mimetypesFileTypeMap.getContentType(fileName);
    }
}
