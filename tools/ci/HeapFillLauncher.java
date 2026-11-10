import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Starts the server and, once its Flight port accepts connections, fills the heap to a share of
 * -Xmx and holds it (CI memory-pressure check of item 40(a) / Q-40a). Usage:
 * java -Dfill.port=PORT -Dfill.percent=90 -cp CLASSPATH HeapFillLauncher.java SERVER_ARGS...
 */
public final class HeapFillLauncher {

    /** Chunk of one allocation: below half a G1 region, so no humongous waste. */
    private static final int CHUNK = 256 << 10;

    /** Arrays kept alive until the process ends. */
    private static final List<byte[]> HELD = new ArrayList<>();

    private HeapFillLauncher() {
    }

    /**
     * Runs the server and fills the heap after it started.
     *
     * @param args server arguments
     * @throws Exception when the server class cannot be started
     */
    public static void main(String[] args) throws Exception {
        int port = Integer.getInteger("fill.port", 32012);
        int percent = Integer.getInteger("fill.percent", 90);
        Method main = Class.forName("pro.surpin.data.arrowflight.server.HadoopArrowFlightServer")
                .getMethod("main", String[].class);
        Thread server = new Thread(() -> {
            try {
                main.invoke(null, (Object) args);
            } catch (ReflectiveOperationException e) {
                e.printStackTrace();
                System.exit(2);
            }
        }, "server-main");
        server.start();
        while (!listening(port)) {
            Thread.sleep(500);
        }
        Runtime rt = Runtime.getRuntime();
        long target = rt.maxMemory() / 100 * percent;
        try {
            while (rt.totalMemory() - rt.freeMemory() < target) {
                byte[] chunk = new byte[CHUNK];
                for (int i = 0; i < chunk.length; i += 4096) {
                    chunk[i] = 1;
                }
                HELD.add(chunk);
            }
        } catch (OutOfMemoryError e) {
            HELD.remove(HELD.size() - 1);
            System.out.println("heapFill stopped by OutOfMemoryError");
        }
        System.out.println("heapFilled usedMiB=" + ((rt.totalMemory() - rt.freeMemory()) >> 20)
                + " maxMiB=" + (rt.maxMemory() >> 20) + " chunks=" + HELD.size());
        server.join();
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Tells whether a local port accepts connections.
     *
     * @param port TCP port
     * @return true when a connection succeeded
     */
    private static boolean listening(int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", port), 200);
            return true;
        } catch (java.io.IOException e) {
            return false;
        }
    }
}
