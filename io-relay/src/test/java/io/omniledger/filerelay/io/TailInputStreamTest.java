package io.omniledger.filerelay.io;

import io.omniledger.filerelay.FileServiceExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
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

    @RepeatedTest(100)
    public void test() throws Exception {
        // context-path is cleaned up after each test anyway, so this doesn't litter
        File appended = newFile();

        TailInputStream inputStream =
                new TailInputStream(
                        testContext.fileService(),
                        appended,
                        // terminate waiting and signal EOF after 2 seconds
                        2000
                );

        writeAndReadFileFully(inputStream, appended);
    }

    /**
     * Buffering works well usually, but not when looking for immediate results.
     * Still, we should be able to handle the lag from it. Unfortunately, this isn't
     * automatically true, so tests can become flaky.
     * */
    @RepeatedTest(10)
    public void testBuffered() throws Exception {
        File appended = newFile();

        InputStream inputStream =
                new BufferedInputStream(
                    new TailInputStream(
                        testContext.fileService(),
                        appended,
                        // terminate waiting and signal EOF after 2 seconds
                        2000
                    )
                );

        writeAndReadFileFully(inputStream, appended);
    }

    private static File newFile() throws IOException {
        // context-path is cleaned up after each test anyway, so this doesn't litter
        File appended = new File(testContext.basePath().toFile(), "foo");
        boolean created = appended.createNewFile();
        assert created;
        return appended;
    }

    private void writeAndReadFileFully(InputStream inputStream, File appended) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<byte[]> readFuture = readFileOnDifferentThread(inputStream);

        writeFileInSmallChunks(appended, readFuture);
        LOGGER.debug("Finished writing file {}", appended);

        // test that read-future completes once the whole buffer has been written out
        readFuture.get(1, TimeUnit.SECONDS);
    }


    private static void writeFileInSmallChunks(
            File appended,
            CompletableFuture<byte[]> readFuture
    )
    throws IOException
    {
        int wrote = 0;
        byte[] bytes = new byte[64];

        // event doesn't trigger until we close the outputstream, so yeah...
        while (wrote < TEST_LENGTH) {
            // append file on this thread and check that read-future isn't complete yet
            try(OutputStream oos = new FileOutputStream(appended, true)) {
                if(readFuture.isDone()) {
                    if(readFuture.isCompletedExceptionally()) {
                        try {
                            readFuture.get();
                            // we have an exception, it shouldn't end up here
                            throw new IllegalStateException();
                        }
                        catch (ExecutionException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    else {
                        Assertions.fail(
                            "Read-future shouldn't have completed normally until file has been written fully! " +
                            "Only wrote " + wrote + "/" + TEST_LENGTH
                        );
                    }
                }

                // file should be fully written using the buffer multiple times
                assert TEST_LENGTH % bytes.length == 0;
                new Random().nextBytes(bytes);
                oos.write(bytes);
                oos.flush();
                oos.close();
                wrote += bytes.length;

                LOGGER.debug("Wrote bytes {}/{}", wrote, TEST_LENGTH);
            }
        }
    }

    private CompletableFuture<byte[]> readFileOnDifferentThread(InputStream inputStream) {
        // read contents of file until EOF on a different thread
        CompletableFuture<byte[]> contentFuture = new CompletableFuture<>();
        final CountDownLatch latch = new CountDownLatch(1);

        Runnable r =
            () -> {
                int readSoFar = 0;
                byte[] buffer = new byte[TEST_LENGTH];
                try {
                    while (readSoFar < TEST_LENGTH) {
                        int toRead = TEST_LENGTH - readSoFar;

                        // this is ignored after the first loop (since it was initiated with 1)
                        latch.countDown();

                        assert readSoFar >= 0;
                        assert toRead > 0;
                        assert buffer.length == toRead + readSoFar;

                        // we don't expect an EOF from this inputstream
                        int read = inputStream.read(buffer, readSoFar, toRead);

                        // this is an EOF, so a timeout
                        assert read != -1;

                        readSoFar += read;
                        LOGGER.debug("Read {}/{}", readSoFar, TEST_LENGTH);
                    }
                    LOGGER.debug("Completing future, having read {} bytes", readSoFar);
                    contentFuture.complete(buffer);
                } catch (Exception e) {
                    LOGGER.debug("Error reading future, having read {} bytes", readSoFar);
                    contentFuture.completeExceptionally(e);
                }
            };

        // just start a new thread that terminates when done
        Thread t = new Thread(r);
        t.setUncaughtExceptionHandler
            ((thr, e) -> {
                LOGGER.error("Error", e);
                contentFuture.completeExceptionally(e);
            }
        );

        t.start();

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

