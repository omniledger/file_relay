package io.omniledger.filerelay.io;

import io.omniledger.filerelay.FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
        return fileInputStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int available = super.available();
        if(available > 0) {
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("Reading {} bytes from file {}", available, file);
            }
            readSoFar += available;
            return super.read(b, off, len);
        }

        if(LOGGER.isTraceEnabled()) {
            LOGGER.trace("Zero bytes available in file, waiting for appends {}", file);
        }

        // nothing available, block
        CompletableFuture<Void> readFuture = new CompletableFuture<>();

        Runnable listener = () -> readFuture.complete(null);
        fileService.onFileChange(file.toPath(), listener);

        // refresh underlying FIS
        refreshInputStream();
        available = fileInputStream.available();
        if(available > 0) {
            // finish future just to be nice
            readFuture.complete(null);
            return super.read(b, off, len);
        }
        else {
            try {
                readFuture.get(timeout, TimeUnit.MILLISECONDS);
                refreshInputStream();

                // normal future finish, read again
                return read(b, off, len);
            } catch (InterruptedException | ExecutionException e) {
                // if waiting was forcibly interrupted or there was an error in executing, nothing we can do
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Timed out waiting for file {} to be appended after {} ms", file, timeout);
                }

                // if this was a timeout, we haven't received new bytes in the time, return 0
                return 0;
            }
        }
    }

    private void refreshInputStream() throws IOException {
        this.fileInputStream.close();
        this.fileInputStream = new FileInputStream(file);
        fileInputStream.skip(readSoFar);
    }
}
