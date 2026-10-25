package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Hive Metastore over Thrift (the generated client of Hive 3.1.3, which HMS 2.x to 4.x answer
 * for these calls), with a small pool of connections. A broken connection is dropped and the
 * call is tried once more on a new one, starting with the URI that worked last and going on to
 * the next ones. Authentication: none, or Kerberos over the SASL transport (GSSAPI, as Hive's
 * own client with {@code hive.metastore.sasl.enabled}).
 */
final class ThriftMetastore implements Metastore {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftMetastore.class);

    private final List<String> uris;
    private final int connectTimeoutMillis;
    private final int socketTimeoutMillis;
    private final boolean framed;
    private final HiveSettings.Kerberos kerberos;
    private final KerberosLogin login;
    private final Semaphore permits;
    private final Deque<Connection> idle = new ArrayDeque<>();
    private volatile int preferred;
    private volatile boolean requestCalls = true;
    private volatile boolean closed;
    /** Until when calls fail at once after the metastore could not be reached (epoch ms). */
    private volatile long unreachableUntil;
    private volatile String unreachableError;

    /**
     * How long calls fail at once after the metastore could not be reached: one request makes
     * many calls (schemas, tables, partitions), and a metastore that hangs would cost each of
     * them the connect and socket timeouts.
     */
    static final long FAIL_FAST_MILLIS = 5_000;

    /**
     * Largest metastore response read (1 GiB, Hive 4's client default): libthrift's own default
     * of 100 MB would turn a large partition list into an error, 0.9.3 had no limit at all.
     */
    static final int MAX_MESSAGE_BYTES = 1 << 30;

    /**
     * Returns the Thrift limits of a new connection: frame and recursion limits stay libthrift's
     * (16 MB frames as with 0.9.3, depth 64).
     *
     * @return configuration
     */
    static TConfiguration thriftConfiguration() {
        return TConfiguration.custom().setMaxMessageSize(MAX_MESSAGE_BYTES).build();
    }

    /** One open connection. */
    private static final class Connection {
        private final String uri;
        private final TTransport transport;
        private final ThriftHiveMetastore.Client client;

        Connection(String uri, TTransport transport) {
            this.uri = uri;
            this.transport = transport;
            this.client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
        }

        void close() {
            transport.close();
        }
    }

    /**
     * One call on a client.
     *
     * @param <T> result type
     */
    @FunctionalInterface
    interface Call<T> {
        /**
         * Makes the call.
         *
         * @param client client
         * @return result
         * @throws TException on Thrift or metastore errors
         */
        T apply(ThriftHiveMetastore.Client client) throws TException;
    }

    /**
     * Creates the metastore; connects on first use.
     *
     * @param settings settings
     */
    ThriftMetastore(HiveSettings settings) {
        this.uris = settings.uris();
        this.connectTimeoutMillis = settings.connectTimeoutMillis();
        this.socketTimeoutMillis = settings.socketTimeoutMillis();
        this.framed = settings.framedTransport();
        this.kerberos = settings.kerberos();
        this.login = kerberos == null ? null
                : new KerberosLogin(kerberos.clientPrincipal(), kerberos.clientKeytab(),
                        kerberos.clientTicketCache());
        this.permits = new Semaphore(settings.maxConnections(), true);
    }

    @Override
    public List<String> databases() throws IOException {
        return call("get_all_databases", ThriftHiveMetastore.Client::get_all_databases);
    }

    @Override
    public List<String> tables(String database) throws IOException {
        List<String> result = callOrMissing("get_all_tables " + database,
                client -> client.get_all_tables(database));
        return result == null ? List.of() : result;
    }

    @Override
    public List<Table> tableObjects(String database, List<String> names) throws IOException {
        if (names.isEmpty()) {
            return List.of();
        }
        List<Table> result;
        if (requestCalls) {
            try {
                // With capabilities: HMS 3 refuses insert-only (ACID) tables to clients that do
                // not declare them, and that refusal would fail the whole database.
                GetTablesRequest request = new GetTablesRequest(database);
                request.setTblNames(names);
                request.setCapabilities(capabilities());
                GetTablesResult answer = callOrMissing("get_table_objects_by_name_req " + database,
                        client -> client.get_table_objects_by_name_req(request));
                return answer == null || answer.getTables() == null ? List.of() : answer.getTables();
            } catch (UnknownMethod e) {
                requestCalls = false;   // HMS before 2.3
            }
        }
        result = callOrMissing("get_table_objects_by_name " + database,
                client -> client.get_table_objects_by_name(database, names));
        return result == null ? List.of() : result;
    }

    @Override
    public Optional<Table> table(String database, String name) throws IOException {
        if (requestCalls) {
            try {
                GetTableRequest request = new GetTableRequest(database, name);
                request.setCapabilities(capabilities());
                GetTableResult answer = callOrMissing("get_table_req " + database + "." + name,
                        client -> client.get_table_req(request));
                return answer == null ? Optional.empty() : Optional.ofNullable(answer.getTable());
            } catch (UnknownMethod e) {
                requestCalls = false;
            }
        }
        return Optional.ofNullable(callOrMissing("get_table " + database + "." + name,
                client -> client.get_table(database, name)));
    }

    @Override
    public List<String> partitionNames(String database, String table) throws IOException {
        List<String> result = callOrMissing("get_partition_names " + database + "." + table,
                client -> client.get_partition_names(database, table, (short) -1));
        return result == null ? List.of() : result;
    }

    @Override
    public List<Partition> partitions(String database, String table, List<String> names)
            throws IOException {
        if (names.isEmpty()) {
            return List.of();
        }
        List<Partition> result = callOrMissing("get_partitions_by_names " + database + "." + table,
                client -> client.get_partitions_by_names(database, table, names));
        return result == null ? List.of() : result;
    }

    /**
     * Declares what this client handles with table reads: insert-only tables (it skips them).
     *
     * @return capabilities
     */
    private static ClientCapabilities capabilities() {
        return new ClientCapabilities(new ArrayList<>(List.of(ClientCapability.INSERT_ONLY_TABLES)));
    }

    @Override
    public void close() {
        closed = true;
        synchronized (idle) {
            for (Connection connection : idle) {
                connection.close();
            }
            idle.clear();
        }
    }

    /**
     * Makes a call; a missing object is null.
     *
     * @param what call, for messages
     * @param call call
     * @param <T> result type
     * @return result, or null when the metastore says the object does not exist
     * @throws IOException when the metastore cannot be reached or fails
     */
    <T> T callOrMissing(String what, Call<T> call) throws IOException {
        try {
            return call(what, call);
        } catch (MissingObject e) {
            return null;
        }
    }

    /** The metastore does not know a call (an older version). */
    private static final class UnknownMethod extends IOException {
        private static final long serialVersionUID = 1L;

        UnknownMethod(String message) {
            super(message);
        }
    }

    /** The metastore said the object does not exist. */
    private static final class MissingObject extends IOException {
        private static final long serialVersionUID = 1L;

        MissingObject(String message) {
            super(message);
        }
    }

    /**
     * Makes a call; for {@link #FAIL_FAST_MILLIS} after the metastore could not be reached,
     * calls fail at once with that error.
     *
     * @param what call, for messages
     * @param call call
     * @param <T> result type
     * @return result
     * @throws IOException when the metastore cannot be reached or fails
     */
    <T> T call(String what, Call<T> call) throws IOException {
        if (closed) {
            throw new IOException("The Hive Metastore client is closed");
        }
        if (System.currentTimeMillis() < unreachableUntil) {
            throw new IOException("Hive Metastore unavailable (" + String.join(",", uris) + ") for "
                    + what + ": " + unreachableError + " (not retried for " + FAIL_FAST_MILLIS
                    + " ms after the failure)");
        }
        try {
            T result = callPooled(what, call);
            unreachableUntil = 0;
            return result;
        } catch (Unreachable e) {
            unreachableError = e.getMessage();
            unreachableUntil = System.currentTimeMillis() + FAIL_FAST_MILLIS;
            throw new IOException(e.getMessage(), e.getCause());
        }
    }

    /** The metastore could not be reached (no connection, or the connection broke twice). */
    private static final class Unreachable extends IOException {
        private static final long serialVersionUID = 1L;

        Unreachable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Makes a call on a pooled connection, once more on a new connection when it breaks.
     *
     * @param what call, for messages
     * @param call call
     * @param <T> result type
     * @return result
     * @throws Unreachable when the metastore cannot be reached
     * @throws IOException when the metastore fails the call
     */
    private <T> T callPooled(String what, Call<T> call) throws IOException {
        boolean acquired;
        try {
            acquired = permits.tryAcquire(connectTimeoutMillis + (long) socketTimeoutMillis,
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for a Hive Metastore connection", e);
        }
        if (!acquired) {
            throw new IOException("No Hive Metastore connection became free for " + what);
        }
        try {
            Exception last = null;
            for (int attempt = 0; attempt < 2; attempt++) {
                Connection connection;
                try {
                    connection = borrow();
                } catch (IOException e) {
                    throw new Unreachable(e.getMessage(), e);
                }
                try {
                    T result = call.apply(connection.client);
                    giveBack(connection);
                    return result;
                } catch (NoSuchObjectException e) {
                    giveBack(connection);
                    throw new MissingObject(what + ": " + e.getMessage());
                } catch (TTransportException | TProtocolException e) {
                    connection.close();
                    // The idle connections are likely broken the same way (metastore restarted
                    // or hanging): the retry opens a new one instead of waiting on each of them.
                    closeIdle();
                    last = e;
                    LOGGER.debug("hive=metastoreConnectionBroken uri={} call={} error={}",
                            connection.uri, what, e.toString());
                } catch (TApplicationException e) {
                    giveBack(connection);
                    if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                        throw new UnknownMethod(what + ": " + e.getMessage());
                    }
                    throw new IOException("Hive Metastore " + connection.uri + " failed " + what
                            + ": " + e, e);
                } catch (TException | RuntimeException e) {
                    // The metastore answered with an error: the connection is still good.
                    giveBack(connection);
                    throw new IOException("Hive Metastore " + connection.uri + " failed " + what
                            + ": " + e, e);
                }
            }
            throw new Unreachable("Hive Metastore unavailable (" + String.join(",", uris)
                    + ") for " + what + ": " + last, last);
        } finally {
            permits.release();
        }
    }

    /**
     * Takes an idle connection or opens a new one, trying the URIs from the one that worked
     * last.
     *
     * @return connection
     * @throws IOException when no URI can be reached
     */
    private Connection borrow() throws IOException {
        synchronized (idle) {
            Connection connection = idle.poll();
            if (connection != null) {
                return connection;
            }
        }
        List<String> errors = new ArrayList<>();
        int start = preferred;
        for (int i = 0; i < uris.size(); i++) {
            int index = (start + i) % uris.size();
            String uri = uris.get(index);
            try {
                Connection connection = open(uri);
                preferred = index;
                return connection;
            } catch (IOException | TTransportException | RuntimeException e) {
                errors.add(uri + ": " + e.getMessage());
            }
        }
        throw new IOException("Hive Metastore unavailable: " + String.join("; ", errors));
    }

    /** Closes every idle connection. */
    private void closeIdle() {
        synchronized (idle) {
            for (Connection connection = idle.poll(); connection != null; connection = idle.poll()) {
                connection.close();
            }
        }
    }

    /**
     * Returns a connection to the pool.
     *
     * @param connection connection
     */
    private void giveBack(Connection connection) {
        synchronized (idle) {
            if (closed) {
                connection.close();
            } else {
                idle.push(connection);
            }
        }
    }

    /**
     * Opens a connection.
     *
     * @param uri {@code thrift://host:port}
     * @return connection
     * @throws TTransportException when it cannot be opened
     * @throws IOException when the Kerberos login fails
     */
    private Connection open(String uri) throws TTransportException, IOException {
        URI parsed = URI.create(uri);
        int port = parsed.getPort() < 0 ? 9083 : parsed.getPort();
        TSocket socket = new TSocket(thriftConfiguration(), parsed.getHost(), port,
                socketTimeoutMillis, connectTimeoutMillis);
        TTransport transport;
        if (kerberos != null) {
            transport = saslOpen(socket, parsed.getHost(), login.subject());
        } else {
            transport = framed ? new TFramedTransport(socket) : socket;
            transport.open();
        }
        LOGGER.debug("hive=metastoreConnected uri={}", uri);
        return new Connection(uri, transport);
    }

    /**
     * Opens the SASL (GSSAPI) transport over a socket as the logged-in subject.
     *
     * @param socket socket, not yet open
     * @param host metastore host, for {@code _HOST} in the metastore principal
     * @param subject Kerberos login
     * @return open transport
     * @throws TTransportException when the connection or the SASL handshake fails
     */
    private TTransport saslOpen(TSocket socket, String host, Subject subject)
            throws TTransportException {
        String[] service = KerberosLogin.servicePrincipal(kerberos.metastorePrincipal(), host);
        String peer = service[0] + "/" + service[1];
        try {
            // The GSSAPI client takes its credentials from the subject: create and open in it.
            return Subject.callAs(subject, () -> {
                TSaslClientTransport transport = new TSaslClientTransport("GSSAPI", null,
                        service[0], service[1],
                        Map.of(Sasl.QOP, kerberos.qop(), Sasl.SERVER_AUTH, "true"), null, socket);
                transport.open();
                return transport;
            });
        } catch (CompletionException e) {
            socket.close();
            Throwable cause = e.getCause() == null ? e : e.getCause();
            String message = cause instanceof TTransportException || cause instanceof SaslException
                    ? cause.getMessage() : cause.toString();
            throw new TTransportException("Kerberos (SASL) handshake with " + peer + " failed: "
                    + message, cause);
        }
    }
}
