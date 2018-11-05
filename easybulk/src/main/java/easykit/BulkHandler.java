package easykit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class BulkHandler<T> {
    private final int bulkActions;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ScheduledFuture<?> scheduledFuture;
    private final ExecutorService processor;
    private final LinkedBlockingQueue<T> items;
    private final int maxRetryNum;
    private final int concurrentLevel;

    private Consumer<Collection<T>> operation;
    private Listener<T> listener;

    private volatile boolean closed = false;

    public interface Listener<T> {

        /**
         * Callback before the bulk is executed.
         */
        void beforeBulk(List<T> footprints);

        /**
         * Callback after a successful execution of bulk request.
         */
        void afterBulk(List<T> footprints);

        /**
         * Callback after a failed execution of bulk request.
         */
        void afterBulk(List<T> footprints, Throwable failure);
    }

    public static class Builder {

        private Consumer operation;
        private Listener listener;

        private int bulkActions = 1000;//default max 1000 records/flush
        private long flushInterval = TimeUnit.MILLISECONDS.toMillis(100);//default max 100ms/flush
        private int maxRetryNum;
        private int concurrentLevel;

        public Builder setListener(Listener listener) {
            this.listener = listener;
            return this;
        }

        public Builder setOperation(Consumer operation) {
            this.operation = operation;
            return this;
        }

        public Builder setBulkActions(int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        public Builder setFlushInterval(long interval, TimeUnit timeUnit) {
            this.flushInterval = timeUnit.toMillis(interval);
            return this;
        }

        public Builder setRetry(int maxRetryNum) {
            this.maxRetryNum = maxRetryNum;
            return this;
        }

        public Builder setConcurrentLevel(int concurrentLevel) {
            this.concurrentLevel = concurrentLevel;
            return this;
        }

        /**
         * Builds a new bulk processor.
         */
        public <T> BulkHandler<T> build() {
            return new BulkHandler<T>(operation, listener, bulkActions, flushInterval, maxRetryNum, concurrentLevel);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private BulkHandler(Consumer<Collection<T>> operation, Listener<T> listener, int bulkActions, long flushInterval, int maxRetryNum, int concurrentLevel) {
        this.bulkActions = bulkActions;

        this.operation = operation;
        this.listener = listener;
        this.items = new LinkedBlockingQueue<>();
        this.maxRetryNum = maxRetryNum;
        this.concurrentLevel = concurrentLevel;

        this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval, flushInterval, TimeUnit.MILLISECONDS);

        this.processor = Executors.newWorkStealingPool(concurrentLevel);
    }

    public void close() {
        awaitClose();
    }

    private synchronized void awaitClose() {
        if (closed) {
            return;
        }
        closed = true;
        if (this.scheduledFuture != null) {
            this.scheduledFuture.cancel(false);
            this.scheduler.shutdown();
            try {
                this.scheduler.awaitTermination(1, TimeUnit.MINUTES);
            } catch (Exception ignored) {
            } finally {
                this.scheduler.shutdownNow();
            }
        }
        if (items.size() > 0) {
            execute();
        }
    }

    public BulkHandler add(T request) {
        internalAdd(request);
        return this;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    private synchronized void internalAdd(T request) {
        ensureOpen();
        items.add(request);
        executeIfNeeded();
    }

    private void executeIfNeeded() {
        ensureOpen();
        if (!isOverTheLimit()) {
            return;
        }

        Future future = processor.submit((Runnable) this::execute);
        if (concurrentLevel == 1) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void execute() {
        int retry = 0;
        List<T> bulkRecords = new ArrayList<>();
        this.items.drainTo(bulkRecords);
        boolean needRetry = execute(bulkRecords);
        while (needRetry && retry++ < this.maxRetryNum) {
            needRetry = execute(bulkRecords);
        }
    }

    private boolean execute(List<T> bulkRecords) {
        listener.beforeBulk(bulkRecords);
        boolean operationFinished = false;
        try {
            this.operation.accept(bulkRecords);
            operationFinished = true;
            listener.afterBulk(bulkRecords);
            return false;
        } catch (Exception e) {
            if (!operationFinished) {
                listener.afterBulk(bulkRecords, e);
                return true;
            }
            return false;
        }
    }

    private boolean isOverTheLimit() {
        return bulkActions != -1 && items.size() >= bulkActions;
    }

    class Flush implements Runnable {

        @Override
        public void run() {
            synchronized (BulkHandler.this) {
                if (closed) {
                    return;
                }
                if (items.isEmpty()) {
                    return;
                }
                execute();
            }
        }
    }
}
