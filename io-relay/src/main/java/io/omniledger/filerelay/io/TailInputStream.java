package io.omniledger.filerelay.io;

import io.omniledger.filerelay.FileService;
import io.omniledger.filerelay.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.*;

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

    public TailInputStream(
            FileService fileService,
            String file,
            long timeout
    )
    throws FileNotFoundException
    {
        this(fileService, new File(file), timeout);
    }

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
        int result =
            blockUntilDataAvailable(
                    () -> {
                        assert fileInputStream.available() > 0;
                        int r = fileInputStream.read();
                        assert r != -1;
                        return r;
                    }
            );

        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result =
            blockUntilDataAvailable(
                () -> {
                    assert fileInputStream.available() > 0;
                    int r = fileInputStream.read(b, off, len);
                    assert r != -1;
                    return r;
                }
            );

        return result;
    }

    private int blockUntilDataAvailable(ThrowingSupplier<IOException, Integer> r) throws IOException {
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

        CountDownLatch latch = new CountDownLatch(1);

        // nothing available, register listener
        fileService.onFileChange(file.toPath(), latch::countDown);

        /*
        * We need to retry reading stuff from the file, since data might have
        * arrived in it between last checking it and now. Since input-streams in-between
        * might have already decided that the file was finished, we need to recycle them
        * first
        * */
        refreshInputStream();
        available = fileInputStream.available();

        // if it now has available bytes, return
        if(available > 0) {
            if(LOGGER.isTraceEnabled()) {
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
        else {
            try {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Entering wait-state for data on file {}", file);
                }

                // otherwise wait for signal from latch
                if(!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    if(LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Timed out waiting for file {} to be appended after {} ms", file, timeout);
                    }
                    // if this was a timeout, we haven't received new bytes in the time, return 0
                    return 0;
                }

                // refresh inputstream, since it might have been marked as finished when we checked "available"
                refreshInputStream();

                // we were woken up normally, but the value is not available on the new stream yet, try again
                if(fileInputStream.available() == 0) {
                    LOGGER.warn("No data available after file-service notification, trying again");
                    return blockUntilDataAvailable(r);
                }

                // normal future finish, read again
                Integer result = r.get();
                assert result != null && result != -1 : result;
                readSoFar += result;
                return result;
            } catch (InterruptedException e) {
                // if waiting was forcibly interrupted or there was an error in executing, nothing we can do
                throw new RuntimeException(e);
            }
        }
    }

    private void blockUntilDataAvailable() {

    }

    private void refreshInputStream() throws IOException {
        this.fileInputStream.close();
        this.fileInputStream = new FileInputStream(file);
        fileInputStream.skip(readSoFar);
    }
}
