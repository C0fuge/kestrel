package pro.surpin.data.arrowflight.server.plugin;

/**
 * Sets the thread context class loader for the length of a call into a plugin and puts the
 * previous one back: {@code try (ThreadContext ignored = ThreadContext.of(loader)) { ... }}.
 * The AWS SDK, Iceberg and Hive find their own classes through it.
 */
public final class ThreadContext implements AutoCloseable {

    private final Thread thread;
    private final ClassLoader previous;
    private final boolean switched;

    /**
     * Switches the current thread's context class loader.
     *
     * @param loader loader to set, or null to leave it alone
     */
    private ThreadContext(ClassLoader loader) {
        this.thread = Thread.currentThread();
        this.previous = thread.getContextClassLoader();
        this.switched = loader != null && loader != previous;
        if (switched) {
            thread.setContextClassLoader(loader);
        }
    }

    /**
     * Sets a loader as the context class loader of the current thread.
     *
     * @param loader plugin loader, or null for the server's own code
     * @return scope that restores the previous loader when closed
     */
    public static ThreadContext of(ClassLoader loader) {
        return new ThreadContext(loader);
    }

    @Override
    public void close() {
        if (switched) {
            thread.setContextClassLoader(previous);
        }
    }
}
