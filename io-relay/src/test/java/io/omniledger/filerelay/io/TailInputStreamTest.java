package io.omniledger.filerelay.io;

import io.omniledger.filerelay.FileServiceExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;

public class TailInputStreamTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TailInputStreamTest.class);

    private static final int FILE_LENGTH = 64 * 1024;
    private static final int READ_BUFFER_LENGTH = 512;

    @RegisterExtension
    static FileServiceExtension testContext = new FileServiceExtension();

    @RepeatedTest(10)
    public void test() throws Throwable {
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

    @RepeatedTest(10)
    public void testTimeout() throws Exception {
        // context-path is cleaned up after each test anyway, so this doesn't litter
        File appended = newFile();

        TailInputStream inputStream =
                new TailInputStream(
                        testContext.fileService(),
                        appended,
                        // terminate waiting and signal EOF after 100 milliseconds
                        100
                );

        // read 10 more bytes than will be written
        CompletableFuture<byte[]> readFuture = readFileOnDifferentThread(inputStream, FILE_LENGTH + 10);

        /*
         readFuture should time out eventually, write is sync, so already finished
         we expect a Timeout to be thrown where the stream is being read, which should
         be reflected in the future, not on this thread. If the future doesn't get that
         exception in 3 seconds, it will be given here
         */
        try {
            writeFileInSmallChunks(appended, readFuture);
            readFuture.get(10, TimeUnit.SECONDS);
            Assertions.fail("Should have received an execution-exception");
        }
        catch (ExecutionException e) {
            if(!(e.getCause() instanceof IOException)) {
                Assertions.fail("Invalid exception " + e);
                assertIOTimeout((IOException) e.getCause());
            }
        }
        catch (IOException e) {
            assertIOTimeout(e);
        }
        catch (Throwable e) {
            Assertions.fail("Expected either IOException or a packaging instance");
        }
    }

    private void assertIOTimeout(IOException e) {
        if(!(e.getCause() instanceof TimeoutException)) {
            LOGGER.error("Invalid exception", e);
            Assertions.fail("Invalid exception " + e);
        }
    }

    @Test
    public void testStreamBasics() throws Throwable {
        File file = newFile();
        CompletableFuture<byte[]> testFuture = new CompletableFuture<>();
        writeFileInSmallChunks(file, testFuture);

        // test that TIS can be used as a basic file-input-stream for existing files
        TailInputStream inputStream = new TailInputStream(testContext.fileService(), file, 10);
        Assertions.assertEquals(FILE_LENGTH, inputStream.available());
        byte[] buffer = new byte[FILE_LENGTH];
        inputStream.read(buffer, 0, buffer.length);
    }

    /**
     * Buffering works well usually, but not when looking for immediate results.
     * Still, we should be able to handle the lag from it. Unfortunately, this isn't
     * automatically true, so tests can become flaky.
     * */
    @RepeatedTest(10)
    public void testBuffered() throws Throwable {
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

    private void writeAndReadFileFully(InputStream inputStream, File appended) throws Throwable{
        CompletableFuture<byte[]> readFuture = readFileOnDifferentThread(inputStream, FILE_LENGTH);

        byte[] wrote = writeFileInSmallChunks(appended, readFuture);
        LOGGER.debug("Finished writing file {}", appended);

        // test that read-future completes once the whole buffer has been written out
        byte[] read = readFuture.get(1, TimeUnit.SECONDS);

        // test that we received the  same file we wrote
        Assertions.assertArrayEquals(wrote, read);
    }


    private static byte[] writeFileInSmallChunks(
            File appended,
            CompletableFuture<byte[]> readFuture
    )
    throws Throwable
    {
        int wrote = 0;
        byte[] bytes = new byte[READ_BUFFER_LENGTH];
        ByteArrayOutputStream fileContents = new ByteArrayOutputStream(FILE_LENGTH);

        while (wrote < FILE_LENGTH) {
            // if future has been cancelled in the meantime, we should also give up
            if(readFuture.isDone()) {
                if(readFuture.isCompletedExceptionally()) {
                    try {
                        readFuture.get();
                    }
                    catch (ExecutionException e) {
                        throw e.getCause();
                    }

                    // we have an exception, it should have been thrown out on get
                    throw new IllegalStateException();
                }
                else {
                    Assertions.fail(
                            "Read-future shouldn't have completed normally until file has been written fully! " +
                                    "Only wrote " + wrote + "/" + FILE_LENGTH
                    );
                }
            }

            // append file on this thread and check that read-future isn't complete yet
            try(OutputStream oos = new FileOutputStream(appended, true)) {
                // file should be fully written using the buffer multiple times
                assert FILE_LENGTH % bytes.length == 0;

                new Random().nextBytes(bytes);

                fileContents.write(bytes);
                fileContents.flush();

                oos.write(bytes);
                oos.flush();

                // event doesn't trigger until we close the outputstream
                oos.close();

                wrote += bytes.length;

                LOGGER.debug("Wrote bytes {}/{}", wrote, FILE_LENGTH);
            }
        }
        return fileContents.toByteArray();
    }

    private CompletableFuture<byte[]> readFileOnDifferentThread(InputStream inputStream, int length) {
        // read contents of file until EOF on a different thread
        CompletableFuture<byte[]> contentFuture = new CompletableFuture<>();
        final CountDownLatch latch = new CountDownLatch(1);

        Runnable r =
            () -> {
                int readSoFar = 0;
                byte[] buffer = new byte[length];
                try {
                    while (readSoFar < length) {
                        int toRead = length - readSoFar;

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
                        LOGGER.debug("Read {}/{}", readSoFar, length);
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

