package io.omniledger.filerelay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

public class FileUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    public static void forceDeleteFiles(String logFileDir) {
        Path pathToBeDeleted = Paths.get(logFileDir);

        long startTime = System.currentTimeMillis();
        /*
         sometimes files aren't deleted because the filesystem didn't release the lock in
         good time, so keep trying to delete files for no longer than a few seconds
         */
        while(
            !deleteFiles(pathToBeDeleted)
            && (System.currentTimeMillis() - startTime) > 5_000
        ) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Retrying file-deletes");
        }

        // delete the log-file directory itself
        new File(logFileDir).delete();
    }

    public static List<Path> listFilesRecursively(Path pathToBeDeleted) throws IOException {
        return Files.walk(pathToBeDeleted)
                // it must be sorted, or we could attempt to delete non-empty directories
                .sorted(Comparator.reverseOrder())
                .toList();
    }

    private static boolean deleteFiles(Path pathToBeDeleted) {
        boolean deleted = true;
        try {
            if(!Files.exists(pathToBeDeleted)) {
                LOGGER.warn("Specified path doesn't exist {}", pathToBeDeleted);
                return true;
            }

            // delete each file in the root-dir of the execution
            List<Path> files = listFilesRecursively(pathToBeDeleted);
            for(Path path : files) {
                File file = path.toFile();
                deleted = deleted && file.delete();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return deleted;
    }

}
