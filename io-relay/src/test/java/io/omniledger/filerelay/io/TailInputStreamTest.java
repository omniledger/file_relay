package io.omniledger.filerelay.io;

import io.omniledger.filerelay.FileServiceExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Random;
import java.util.concurrent.*;

public class TailInputStreamTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TailInputStreamTest.class);

    private static final int TEST_LENGTH = 1024;

    @RegisterExtension
    static FileServiceExtension testContext = new FileServiceExtension();

    @Test
    public void test() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        File appended = new File(testContext.basePath().toFile(), "foo");
        boolean created = appended.createNewFile();
        assert created;

        TailInputStream inputStream =
                new TailInputStream(
                        testContext.fileService(),
                        appended,
                        // return "0" as available after 200 ms of no appends
                        200
                );

        CompletableFuture<byte[]> readFuture = readFileOnDifferentThread(inputStream);

        int wrote = 0;
        byte[] bytes = new byte[64];

        // event doesn't trigger until we close the outputstream, so yeah...
        while (wrote < TEST_LENGTH) {
            // append file on this thread and check that read-future isn't complete yet
            try(OutputStream oos = new FileOutputStream(appended)) {
                Assertions.assertFalse(readFuture.isDone());

                // file should be fully written using the buffer multiple times
                assert TEST_LENGTH % bytes.length == 0;
                new Random().nextBytes(bytes);
                oos.write(bytes);
                oos.flush();
                wrote += bytes.length;

                LOGGER.debug("Wrote bytes {}/{}", wrote, TEST_LENGTH);
            }
        }

        LOGGER.debug("Finished writing file {}", appended);

        // test that read-future completes once the whole buffer has been written out
        readFuture.get(1, TimeUnit.SECONDS);
    }

    private CompletableFuture<byte[]> readFileOnDifferentThread(InputStream inputStream) {
        // read contents of file until EOF on a different thread
        CompletableFuture<byte[]> contentFuture = new CompletableFuture<>();

        final CountDownLatch latch = new CountDownLatch(1);

        Runnable r =
            () -> {
                try {
                    byte[] buffer = new byte[TEST_LENGTH];
                    int readSoFar = 0;
                    while (readSoFar < TEST_LENGTH) {
                        int remaining = TEST_LENGTH - readSoFar;

                        // this is ignored after the first loop (since it was initiated with 1)
                        latch.countDown();

                        int read = inputStream.read(buffer, readSoFar, remaining);
                        readSoFar += read;
                        LOGGER.debug("Read {}/{}", readSoFar, TEST_LENGTH);
                    }
                    contentFuture.complete(buffer);
                } catch (Exception e) {
                    contentFuture.completeExceptionally(e);
                }
            };

        // just start a new thread that terminates when done
        new Thread(r).start();

        // wait until thread started and buffer started blocking
        try {
            latch.await();
            LOGGER.debug("File-reader thread started");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return contentFuture;
    }
}

