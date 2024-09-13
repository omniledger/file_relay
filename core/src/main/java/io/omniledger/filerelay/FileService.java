package io.omniledger.filerelay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class FileService implements Runnable, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileService.class);

    private final WatchService watchService;
    private final Thread.UncaughtExceptionHandler onError;
    private final ReentrantLock lock;
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);

    /**
     * On this path, for this kind of event, who's listening?
     * */
    private final Map<Path, List<Consumer<Path>>> fileCreateCallbacks = new HashMap<>();

    private final Map<Path, List<Runnable>> fileModifyCallbacks = new HashMap<>();
    private final Thread thread;
    private final Thread processingThread;
    private final Condition eventsCondition;

    /**
     * @throws IOException if the WatchService couldn't be instantiated.
     * */
    public FileService(Thread.UncaughtExceptionHandler onError) throws IOException {
        this.watchService = FileSystems.getDefault().newWatchService();
        this.onError = onError;
        this.lock = new ReentrantLock();
        this.eventsCondition = lock.newCondition();

        LOGGER.info("Starting file-change listener service");

        this.processingThread = Thread.ofVirtual().unstarted(this::processEvents);
        processingThread.setName("fileservice-process");
        processingThread.setUncaughtExceptionHandler(onError);
        processingThread.start();

        this.thread = Thread.ofVirtual().unstarted(this);
        thread.setName("fileservice-listener");
        thread.setUncaughtExceptionHandler(onError);
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

    public void close() {
        stop();
    }

    /**
     * Shut down thread running file-service and block until it has terminated.
     *
     * @return if this was the request that killed the service.
     * @throws RuntimeException (delegating an {@link InterruptedException}) if the thread shutdown was interrupted
     * */
    public boolean stop() {
        LOGGER.info("Stopping file-change listener service");

        boolean closed = shouldStop.compareAndSet(false, true);
        try {
            processingThread.join();
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return closed;
    }

    private final Map<Path, List<WatchEvent<?>>> eventsPerDirectory = new HashMap<>();

    private record PathEvents(Path path, List<WatchEvent<?>> events) {}

    @Override
    public void run() {
        while(!shouldStop.get()) {
            List<PathEvents> eventsByPath = new ArrayList<>();
            try {
                // check once every 200ms that the environment hasn't been killed yet
                WatchKey watchKey = watchService.poll(200, TimeUnit.MILLISECONDS);

                // it was a timeout
                if(watchKey == null) {
                    continue;
                }

                do {
                    Path dir = (Path) watchKey.watchable();
                    var events = watchKey.pollEvents();
                    eventsByPath.add(new PathEvents(dir, events));

                    watchKey.reset();
                }
                while ((watchKey = watchService.poll()) != null);
            }
            catch (InterruptedException e) {
                LOGGER.error("Error running file-service", e);
                onError.uncaughtException(this.thread, e);
                break;
            }

            try {
                // we have things to do, get the global lock and queue the new events
                lock.lock();
                for(PathEvents events : eventsByPath) {
                    List<WatchEvent<?>> list =
                            eventsPerDirectory.computeIfAbsent(
                                    events.path,
                                    (v) -> new ArrayList<>()
                            );

                    list.addAll(events.events);
                    eventsCondition.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void processEvents() {
        while(!shouldStop.get()) {
            HashMap<Path, List<WatchEvent<?>>> eventMap;
            Map<Path, List<Consumer<Path>>> createCallbacks;
            HashMap<Path, List<Runnable>> modifyCallbacks;

            // clone full state, so that we don't need to block anyone else while processing
            lock.lock();
            try {
                if(eventsPerDirectory.isEmpty()) {
                    boolean result = eventsCondition.await(200, TimeUnit.MILLISECONDS);
                    if(!result) {
                        continue;
                    }
                }

                eventMap = new HashMap<>(eventsPerDirectory);
                createCallbacks = new HashMap<>(fileCreateCallbacks);
                modifyCallbacks = new HashMap<>(fileModifyCallbacks);
                eventsPerDirectory.clear();
            } catch (InterruptedException e) {
                onError.uncaughtException(this.processingThread, e);
                return;
            } finally {
                lock.unlock();
            }

            Map<Path, List<Runnable>> modifiesToDelete = new HashMap<>();
            for (Map.Entry<Path, List<WatchEvent<?>>> entry : eventMap.entrySet()) {
                Path dir = entry.getKey();

                for (WatchEvent<?> event : entry.getValue()) {
                    Path fullPath = dir.resolve((Path) event.context());
                    WatchEvent.Kind<Path> kind = (WatchEvent.Kind<Path>) event.kind();

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("File-event, kind: {} path {} ", kind, fullPath.toFile().getAbsoluteFile());
                    }

                    if (kind == ENTRY_CREATE) {
                        createCallbacks.get(dir).forEach(l -> l.accept(fullPath));
                    } else if (kind == ENTRY_MODIFY) {
                        List<Runnable> callbacks = modifyCallbacks.get(fullPath);
                        modifiesToDelete.computeIfAbsent(fullPath, v -> new ArrayList<>()).addAll(callbacks);

                        for (Runnable callback : callbacks) {
                            callback.run();
                        }
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Triggered {} listeners", callbacks.size());
                        }
                    } else {
                        throw new IllegalStateException("Unknown event kind: " + kind);
                    }
                }
            }

            // file-modification callbacks are single-shot events, so we can get rid of the ones we notified
            if(!modifiesToDelete.isEmpty()) {
                lock.lock();
                try {
                    for(Map.Entry<Path, List<Runnable>> entry : modifiesToDelete.entrySet()) {
                        fileModifyCallbacks.get(entry.getKey()).removeAll(entry.getValue());
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }
}
