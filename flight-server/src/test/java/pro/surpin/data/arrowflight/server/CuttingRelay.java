package pro.surpin.data.arrowflight.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP relay to a Flight server that drops every connection once it has passed a number of bytes
 * from the server, or when the test calls {@link #cutNow()}, as a peer killed in the middle of a
 * stream (the client sees a reset). The relay can pass the server's bytes at a limited rate, so
 * that an answer is still on its way when the test cuts it.
 */
final class CuttingRelay implements AutoCloseable {

    private final ServerSocket listener;
    private final int targetPort;
    private final long cutAfterBytes;
    private final long bytesPerSecond;
    private final AtomicLong relayed = new AtomicLong();
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    private volatile boolean cut;

    /**
     * Starts a relay on a free local port.
     *
     * @param targetPort port of the server on localhost
     * @param cutAfterBytes bytes from the server after which every connection is dropped
     * @throws IOException when the port cannot be opened
     */
    CuttingRelay(int targetPort, long cutAfterBytes) throws IOException {
        this(targetPort, cutAfterBytes, 0);
    }

    /**
     * Starts a relay on a free local port that passes the server's bytes at a limited rate.
     *
     * @param targetPort port of the server on localhost
     * @param cutAfterBytes bytes from the server after which every connection is dropped
     * @param bytesPerSecond rate of the server's bytes, or 0 for no limit
     * @throws IOException when the port cannot be opened
     */
    CuttingRelay(int targetPort, long cutAfterBytes, long bytesPerSecond) throws IOException {
        this.bytesPerSecond = bytesPerSecond;
        this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.targetPort = targetPort;
        this.cutAfterBytes = cutAfterBytes;
        Thread acceptor = new Thread(this::accept, "cutting-relay-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    /**
     * Returns the port clients connect to.
     *
     * @return relay port
     */
    int port() {
        return listener.getLocalPort();
    }

    /**
     * Tells whether the relay has dropped its connections.
     *
     * @return true after the cut
     */
    boolean wasCut() {
        return cut;
    }

    /**
     * Counts the bytes passed from the server so far.
     *
     * @return bytes
     */
    long relayed() {
        return relayed.get();
    }

    /** Resets every connection now. */
    void cutNow() {
        dropAll();
    }

    /**
     * Accepts clients and relays each to the server until the relay closes.
     */
    private void accept() {
        while (!listener.isClosed()) {
            try {
                Socket client = listener.accept();
                Socket server = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                sockets.add(client);
                sockets.add(server);
                pump(client, server, false);
                pump(server, client, true);
            } catch (IOException e) {
                return;
            }
        }
    }

    /**
     * Copies one direction of a connection on a daemon thread.
     *
     * @param from source socket
     * @param to destination socket
     * @param counted whether this direction counts towards the cut (server to client)
     */
    private void pump(Socket from, Socket to, boolean counted) {
        Thread thread = new Thread(() -> {
            byte[] buffer = new byte[8192];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int n;
                while (!cut && (n = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, n);
                    out.flush();
                    if (counted && relayed.addAndGet(n) >= cutAfterBytes) {
                        dropAll();
                    }
                    if (counted && bytesPerSecond > 0) {
                        Thread.sleep(n * 1000L / bytesPerSecond);
                    }
                }
            } catch (IOException e) {
                // the other side or the cut closed the socket
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                closeQuietly(from);
                closeQuietly(to);
            }
        }, "cutting-relay-pump");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Resets every connection (SO_LINGER 0 sends RST, as a killed process' kernel does).
     */
    private void dropAll() {
        cut = true;
        for (Socket socket : sockets) {
            try {
                socket.setSoLinger(true, 0);
            } catch (IOException e) {
                // already closed
            }
            closeQuietly(socket);
        }
    }

    /**
     * Closes a socket, ignoring failures.
     *
     * @param socket socket to close
     */
    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // already closed
        }
    }

    @Override
    public void close() throws IOException {
        listener.close();
        dropAll();
    }
}
