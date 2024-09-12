package io.omniledger.filerelay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class FileService implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileService.class);

    private final WatchService watchService;
    private final Consumer<Throwable> onError;
    private final ReentrantLock lock;
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);

    /**
     * On this path, for this kind of event, who's listening?
     * */
    private final Map<Path, List<Consumer<Path>>> fileCreateCallbacks = new HashMap<>();

    private final Map<Path, List<Runnable>> fileModifyCallbacks = new HashMap<>();
    private final Thread thread;

    public FileService(Consumer<Throwable> onError) throws IOException, InterruptedException {
        this.watchService = FileSystems.getDefault().newWatchService();
        this.onError = onError;
        this.lock = new ReentrantLock();

        LOGGER.info("Starting file-change listener service");

        this.thread = Thread.ofVirtual().unstarted(this);
        thread.setName("fileservice-listener");
        thread.setUncaughtExceptionHandler((t, e) -> onError.accept(e) );
        thread.start();
    }

    public void onFileCreate(Path directory, Consumer<Path> callback) throws IOException {
        assert directory.toFile().isDirectory()
            : "Can only listen to file-creation on a directory! " + directory;

        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("Registering file-create listener for {}", directory);
        }

        lock.lock();
        try {
            directory.register(watchService, ENTRY_CREATE);
            fileCreateCallbacks.computeIfAbsent(directory, k -> new ArrayList<>()).add(callback);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * A one-shot listener that supplies the "next" signal for a specific file when
     * data has been appended to it.
     * */
    public void onFileChange(Path directory, Runnable callback) throws IOException {
        assert directory.toFile().isFile()
            : "Can only listen to file-append events on a file, not a directory! " + directory;

        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("Registering file-change listener for {}", directory);
        }

        lock.lock();
        try {
            directory.getParent().register(watchService, ENTRY_MODIFY);
            fileModifyCallbacks.computeIfAbsent(directory, k -> new ArrayList<>()).add(callback);
        }
        finally {
            lock.unlock();
        }
    }

    public boolean close() {
        LOGGER.info("Stopping file-change listener service");

        boolean closed = shouldStop.compareAndSet(false, true);
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return closed;
    }

    @Override
    public void run() {
        while(!shouldStop.get()) {
            try {
                // check once every 200ms that the environment hasn't been killed yet
                WatchKey watchKey = watchService.poll(200, TimeUnit.MILLISECONDS);
                lock.lock();
                try {
                    Path dir = (Path) watchKey.watchable();
                    List<WatchEvent<?>> events = watchKey.pollEvents();
                    for (WatchEvent<?> event : events) {
                        Path fullPath = dir.resolve((Path) event.context());
                        WatchEvent.Kind<Path> kind = (WatchEvent.Kind<Path>) event.kind();

                        if(LOGGER.isDebugEnabled()) {
                            LOGGER.debug("File-event, kind: {} path {} ", kind, fullPath.toFile().getAbsoluteFile());
                        }

                        if (kind == ENTRY_CREATE) {
                            fileCreateCallbacks.get(dir).forEach(l -> l.accept(fullPath));
                        }
                        else if(kind == ENTRY_MODIFY) {
                            List<Runnable> callbacks = fileModifyCallbacks.remove(fullPath);
                            for(Runnable callback : callbacks) {
                                callback.run();
                            }
                        } else {
                            throw new IllegalStateException("Unknown event kind: " + kind);
                        }
                    }
                }
                finally {
                    watchKey.reset();
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                LOGGER.error("Error running file-service", e);
                onError.accept(e);
                break;
            }
        }
    }
}
