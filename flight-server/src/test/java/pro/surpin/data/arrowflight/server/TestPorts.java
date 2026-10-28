package pro.surpin.data.arrowflight.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Free local ports for test servers, taken below the system's ephemeral port range. A port that
 * {@code new ServerSocket(0)} returned is an ephemeral one: between the probe and the server's
 * bind any outgoing connection of the JVM (a Hazelcast member dialling its seeds, a gRPC client)
 * can get it as its source port, and the server then fails with "Address already in use" (seen
 * in the cluster tests on CI and locally). Ports below the range are only taken by explicit binds.
 */
public final class TestPorts {

    /** Lowest port handed out. */
    static final int FIRST = 20000;

    /** Linux default start of the ephemeral range, used when the system's range is unknown. */
    private static final int DEFAULT_EPHEMERAL_START = 32768;

    private static final Path EPHEMERAL_RANGE = Path.of("/proc/sys/net/ipv4/ip_local_port_range");

    private static final Set<Integer> HANDED_OUT = new HashSet<>();

    private static final int END = ephemeralStart(EPHEMERAL_RANGE);

    private static int next = FIRST + ThreadLocalRandom.current().nextInt(Math.max(1, END - FIRST));

    private TestPorts() {
    }

    /**
     * Returns a port no one listens on, below the ephemeral range and not handed out before in
     * this JVM.
     *
     * @return port
     * @throws IOException when no such port is found
     */
    public static synchronized int free() throws IOException {
        if (END - FIRST < 100) {
            try (ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            }
        }
        for (int tries = 0; tries < END - FIRST; tries++) {
            int port = next;
            next = next + 1 >= END ? FIRST : next + 1;
            if (!HANDED_OUT.contains(port) && bindable(port)) {
                HANDED_OUT.add(port);
                return port;
            }
        }
        throw new IOException("no free port in [" + FIRST + ", " + END + ")");
    }

    /**
     * Reads the start of the ephemeral port range from a Linux range file.
     *
     * @param rangeFile file with "start end", as /proc/sys/net/ipv4/ip_local_port_range
     * @return first ephemeral port, the Linux default when the file cannot be read
     */
    static int ephemeralStart(Path rangeFile) {
        try {
            // A line read: Files.readString saw a truncated /proc file (size 0) in a cloud container.
            try (BufferedReader reader = Files.newBufferedReader(rangeFile, StandardCharsets.US_ASCII)) {
                return Integer.parseInt(reader.readLine().trim().split("\\s+")[0]);
            }
        } catch (IOException | RuntimeException e) {
            return DEFAULT_EPHEMERAL_START;
        }
    }

    /**
     * Returns the end (exclusive) of the range ports are handed out from.
     *
     * @return first port not handed out
     */
    static int end() {
        return END;
    }

    private static boolean bindable(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            return socket.getLocalPort() == port;
        } catch (IOException e) {
            return false;
        }
    }
}
