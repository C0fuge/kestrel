package pro.surpin.data.arrowflight.server.plugin;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Daemon threads for a plugin, with the plugin's loader as their context class loader, so that
 * work a plugin does in the background finds the same classes as its calls.
 */
public final class PluginThreadFactory implements ThreadFactory {

    private final String name;
    private final ClassLoader loader;
    private final AtomicInteger count = new AtomicInteger();

    /**
     * Creates a factory.
     *
     * @param name thread name; a number is appended from the second thread on
     * @param loader plugin loader, or null to keep the creating thread's loader
     */
    public PluginThreadFactory(String name, ClassLoader loader) {
        this.name = Objects.requireNonNull(name, "name");
        this.loader = loader;
    }

    @Override
    public Thread newThread(Runnable task) {
        int n = count.getAndIncrement();
        Thread thread = new Thread(task, n == 0 ? name : name + "-" + n);
        thread.setDaemon(true);
        if (loader != null) {
            thread.setContextClassLoader(loader);
        }
        return thread;
    }
}
