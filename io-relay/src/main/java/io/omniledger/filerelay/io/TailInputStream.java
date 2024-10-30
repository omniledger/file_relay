package io.omniledger.filerelay.io;

import io.omniledger.filerelay.FileService;
import io.omniledger.filerelay.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A lot like a {@link java.io.FileInputStream}.
 * Blocks until either
 * <ul>
 *     <li>file has content not received yet</li>
 *     <li>buffer has some binary data appended to it</li>
 *     <li>or EOF is reached</li>
 * </ul>
 *
 * <p>
 *     Useful when processes communicate with each other via append-only file-streams.
 * </p>
 * */
public class TailInputStream extends InputStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(TailInputStream.class);

    private FileInputStream fileInputStream;
    private final FileService fileService;
    private final File file;
    private final long timeout;
    private int readSoFar = 0;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicReference<CountDownLatch> latchReference = new AtomicReference<>(null);
    private final AtomicBoolean shouldTerminate = new AtomicBoolean(false);

    public TailInputStream(
            FileService fileService,
            File file,
            long timeout
    )
    throws FileNotFoundException
    {
        this.fileInputStream = new FileInputStream(file);
        this.fileService = fileService;
        this.file = file;
        this.timeout = timeout;
    }

    @Override
    public int read() throws IOException {
        return blockUntilDataAvailable(() -> fileInputStream.read() );
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return blockUntilDataAvailable( () -> fileInputStream.read(b, off, len) );
    }

    @Override
    public int available() throws IOException {
        return fileInputStream.available();
    }

    private int blockUntilDataAvailable(ThrowingSupplier<IOException, Integer> r) throws IOException {
        testTerminate();

        int available = fileInputStream.available();
        if(available > 0) {
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("Reading {} bytes from file {}", available, file);
            }
            Integer result = r.get();
            assert result != null && result != -1 : result;
            readSoFar += result;
            return result;
        }

        try {
            CountDownLatch latch;
            lock.lock();

            try {
                // latch-reference might already exist if it's a recursive call
                // assert latchReference.get() == null;
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Setting latch");
                }
                /*
                 we always replace the previous latch, as it might be dirty and it's only about
                 this cycle anyway
                 */
                latchReference.set(latch = new CountDownLatch(1));
            }
            finally {
                lock.unlock();
            }

            // nothing available, register listener
            fileService.onFileChange(
                    file.toPath(),
                    () -> {
                        lock.lock();
                        try {
                            latch.countDown();
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                );

            /*
             * We need to retry reading stuff from the file, since data might have
             * arrived in it between last checking it and now. Since input-streams in-between
             * might have already decided that the file was finished, we need to recycle them
             * first
             * */
            refreshInputStream();
            available = fileInputStream.available();

            // if it has no available bytes, block
            if (available == 0) {
                try {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Entering wait-state for data on file {}", file);
                    }

                    awaitTimeout();

                    if (!fileService.isRunning()) {
                        throw new IOException("File-service stopped");
                    }

                    // refresh inputstream, since it might have been marked as finished when we checked "available"
                    refreshInputStream();

                    // we were woken up normally, but the value is not available on the new stream yet, try again
                    if (fileInputStream.available() == 0) {
                        if(LOGGER.isDebugEnabled()) {
                            LOGGER.debug("No data available after file-service notification, trying again");
                        }
                        return blockUntilDataAvailable(r);
                    }
                } catch (InterruptedException e) {
                    // if waiting was forcibly interrupted or there was an error in executing, nothing we can do
                    throw new RuntimeException(e);
                }
            } else if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                        "{} bytes of data arrived between polling file {} and registering listener",
                        available,
                        file
                );
            }

            Integer result = r.get();
            assert result != null && result != -1 : result;
            readSoFar += result;
            return result;
        }
        finally {
            lock.lock();
            try {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Unsetting latch");
                }

                latchReference.set(null);
            }
            finally {
                lock.unlock();
            }
        }
    }

    private void testTerminate() throws IOException {
        if(this.shouldTerminate.get())  {
            throw new IOException("Terminating input-stream listening");
        }
    }

    public void wakeAndTerminate() {
        if(shouldTerminate.get()) {
            LOGGER.warn("Already terminated");
        }

        lock.lock();
        try {
            shouldTerminate.set(true);
            CountDownLatch latch = latchReference.get();
            if(latch != null) {
                latch.countDown();
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Check if latch finishes in the expected time but also makes sure that thread
     * doesn't block system shutdown.
     * */
    private void awaitTimeout() throws InterruptedException, IOException {
        long started = System.currentTimeMillis();
        while (fileService.isRunning()) {
            // check on server once every 100ms at most
            long elapsedSinceStart = System.currentTimeMillis() - started;
            long remainingUntilTimeout = timeout - elapsedSinceStart;
            long latchTimeout = Math.min(remainingUntilTimeout, 100);

            CountDownLatch latch;
            try {
                lock.lock();
                latch = latchReference.get();

                // if not empty, just carry on, latch will be managed as expected
                if(latch == null) {
                    LOGGER.warn("Reader terminated early?");
                    return;
                }
            }
            finally {
                lock.unlock();
            }

            // if latch is complete, we're good, condition fulfilled
            if (latch.await(latchTimeout, TimeUnit.MILLISECONDS) && !shouldTerminate.get()) {
                return;
            }

            // if it's a termination, drop everything
            testTerminate();

            // latch timed out, check if it's permanent
            if(System.currentTimeMillis() >= started + timeout) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Timed out waiting for file {} to be appended after {} ms", file, timeout);
                }
                // if this was a timeout, we haven't received new bytes in the time, return 0
                throw new IOException(new TimeoutException("File-read: " + file + " timed out after " + timeout + " millis"));
            }

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                        "Woke up to check on server after {} ms, elapse {} ms timeout in {} ms",
                        latchTimeout,
                        System.currentTimeMillis() - started,
                        timeout
                );
            }
        }
    }

    private void refreshInputStream() throws IOException {
        /*
        this.fileInputStream.close();
        this.fileInputStream = new FileInputStream(file);
        long skipped = 0;
        while (skipped < readSoFar) {
            skipped += fileInputStream.skip(readSoFar - skipped);
        }
         */
    }
}
