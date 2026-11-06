package pro.surpin.data.arrowflight.server;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixed pool of Parquet I/O threads that logs when tasks queue up behind busy threads and when
 * a task waited long for a thread.
 */
final class IoPool extends AbstractExecutorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoPool.class);

    /** Shortest time between two {@code pool=queueGrowth} lines (item 36 (4)). */
    static final long QUEUE_GROWTH_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final int parallelism;
    private final ExecutorService delegate;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final AtomicLong submittedTasks = new AtomicLong(0);
    private final LogRateLimit queueGrowthLog = new LogRateLimit(QUEUE_GROWTH_LOG_INTERVAL_NANOS);
    private final AtomicInteger peakQueued = new AtomicInteger(0);

    /**
     * Creates the pool.
     *
     * @param parallelism number of threads
     */
    IoPool(int parallelism) {
        this.parallelism = parallelism;
        this.delegate = Executors.newFixedThreadPool(parallelism, r -> {
            Thread t = new Thread(r, "parquet-io");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void execute(Runnable command) {
        long submitTime = System.nanoTime();
        long taskId = submittedTasks.incrementAndGet();
        int active = activeTasks.incrementAndGet();
        if (active > parallelism) {
            logQueueGrowth(active);
        }
        try {
            delegate.execute(() -> {
                long queueDelay = System.nanoTime() - submitTime;
                if (queueDelay > 1_000_000_000L) {
                    LOGGER.warn("qid={} node={} pool=starvation taskId={} queueDelay={} active={}",
                            LogUtil.qid(), LogUtil.node(), taskId,
                            LogUtil.elapsedNanos(submitTime), activeTasks.get());
                }
                try {
                    command.run();
                } finally {
                    activeTasks.decrementAndGet();
                }
            });
        } catch (RejectedExecutionException e) {
            // The pool is stopping: the task never runs, so it is not active either.
            activeTasks.decrementAndGet();
            throw e;
        }
    }

    /**
     * Logs that tasks queue up, at most once per interval with the peak and the lines held back.
     *
     * @param active tasks submitted and not yet finished
     */
    private void logQueueGrowth(int active) {
        int queued = active - parallelism;
        peakQueued.accumulateAndGet(queued, Math::max);
        long held = queueGrowthLog.tryLog();
        if (held >= 0) {
            LOGGER.warn("node={} pool=queueGrowth active={} max={} queued={} peakQueued={} suppressed={}",
                    LogUtil.node(), active, parallelism, queued, peakQueued.getAndSet(0), held);
        }
    }

    /**
     * Returns the tasks submitted and not yet finished.
     *
     * @return active plus queued tasks
     */
    int activeTasks() {
        return activeTasks.get();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }
}
