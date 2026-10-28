package pro.surpin.data.arrowflight.plugin.hive;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * A real Hive Metastore 3.1.3 in this JVM, on embedded Derby (one per JVM), with a warehouse in
 * a temporary directory; tables, partitions and views are created over Thrift. Used by the
 * plugin's and the server's tests in a class loader over target/fixture-libs: the Hive 3.1.3
 * server needs libthrift 0.9.3, the plugin itself runs on a newer one. Calls only Thrift API
 * that both versions have.
 */
public final class HmsFixture {

    /** Serde parameter key of {@link #addPartition} naming a partition's own serde class. */
    public static final String SERDE_LIB = "(serialization.lib)";

    private static HmsFixture shared;

    private final Path dir;
    private final int port;
    private TTransport transport;
    private ThriftHiveMetastore.Client client;

    private HmsFixture(Path dir, int port) {
        this.dir = dir;
        this.port = port;
    }

    /**
     * Starts the metastore once per JVM.
     *
     * @return the running metastore
     * @throws Exception when it does not start within 120 s
     */
    public static synchronized HmsFixture shared() throws Exception {
        if (shared == null) {
            Path dir = Files.createTempDirectory("hms-fixture");
            int port;
            try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                port = probe.getLocalPort();
            }
            HmsFixture fixture = new HmsFixture(dir, port);
            fixture.start();
            shared = fixture;
        }
        return shared;
    }

    /**
     * Names the metastore.
     *
     * @return {@code thrift://127.0.0.1:<port>}
     */
    public String uri() {
        return "thrift://127.0.0.1:" + port;
    }

    /**
     * Returns the metastore's warehouse directory.
     *
     * @return directory
     */
    public String warehouse() {
        return dir.resolve("wh").toString();
    }

    /**
     * Creates a database (its location below the warehouse) unless it exists.
     *
     * @param name database name
     * @throws TException on metastore errors
     */
    public synchronized void createDatabase(String name) throws TException {
        if (client().get_all_databases().contains(name)) {
            return;
        }
        client().create_database(new Database(name, null, "file:" + warehouse() + "/" + name + ".db",
                null));
    }

    /**
     * Creates an external table.
     *
     * @param db database
     * @param name table name
     * @param location table location (file: URI or path)
     * @param columns {@code name type;name type} (Hive types)
     * @param partitionKeys {@code name type;...}, empty for none
     * @param format "parquet", "text", "csv" (OpenCSVSerde), "json" (HCatalog's JsonSerDe),
     *     "json2" (serde2's JsonSerDe), "openx" (OpenX's JsonSerDe), "avro" or "orc"
     * @param params extra table parameters, or null (with "transactional": a managed table;
     *     keys starting with {@code serde:} go to the serde's parameters without the prefix)
     * @throws TException on metastore errors
     */
    public synchronized void createTable(String db, String name, String location, String columns,
            String partitionKeys, String format, Map<String, String> params) throws TException {
        Map<String, String> parameters = new HashMap<>();
        boolean managed = params != null && params.containsKey("transactional");
        if (!managed) {
            parameters.put("EXTERNAL", "TRUE");
        }
        StorageDescriptor sd = sd(location, fields(columns), format);
        if (params != null) {
            params.forEach((key, value) -> {
                if (key.startsWith("serde:")) {
                    sd.getSerdeInfo().putToParameters(key.substring("serde:".length()), value);
                } else {
                    parameters.put(key, value);
                }
            });
        }
        Table table = new Table(name, db, "test", 0, 0, 0, sd,
                fields(partitionKeys), parameters, null, null,
                managed ? "MANAGED_TABLE" : "EXTERNAL_TABLE");
        client().create_table(table);
    }

    /**
     * Registers an Iceberg table the way Iceberg's HiveCatalog does.
     *
     * @param db database
     * @param name table name
     * @param location table location
     * @param metadataLocation current metadata file
     * @throws TException on metastore errors
     */
    public synchronized void registerIceberg(String db, String name, String location,
            String metadataLocation) throws TException {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("EXTERNAL", "TRUE");
        parameters.put("table_type", "ICEBERG");
        parameters.put("metadata_location", metadataLocation);
        StorageDescriptor sd = sd(location, fields("id bigint"), "iceberg");
        client().create_table(new Table(name, db, "test", 0, 0, 0, sd, new ArrayList<>(), parameters,
                null, null, "EXTERNAL_TABLE"));
    }

    /**
     * Points a registered Iceberg table at a new metadata file (a commit).
     *
     * @param db database
     * @param name table name
     * @param metadataLocation new metadata file
     * @throws TException on metastore errors
     */
    public synchronized void commitIceberg(String db, String name, String metadataLocation)
            throws TException {
        Table table = client().get_table(db, name);
        table.getParameters().put("previous_metadata_location",
                table.getParameters().get("metadata_location"));
        table.getParameters().put("metadata_location", metadataLocation);
        client().alter_table(db, name, table);
    }

    /**
     * Creates a view.
     *
     * @param db database
     * @param name view name
     * @param sql view text
     * @throws TException on metastore errors
     */
    public synchronized void createView(String db, String name, String sql) throws TException {
        Table view = new Table(name, db, "test", 0, 0, 0, sd(null, fields("x int"), "text"),
                new ArrayList<>(), new HashMap<>(), sql, sql, "VIRTUAL_VIEW");
        client().create_table(view);
    }

    /**
     * Creates a view with its expanded text and declared columns.
     *
     * @param db database
     * @param name view name
     * @param expanded expanded view text (tables named with their databases)
     * @param columns declared columns, {@code name type;name type}
     * @throws TException on metastore errors
     */
    public synchronized void createView(String db, String name, String expanded, String columns)
            throws TException {
        Table view = new Table(name, db, "test", 0, 0, 0, sd(null, fields(columns), "text"),
                new ArrayList<>(), new HashMap<>(), expanded, expanded, "VIRTUAL_VIEW");
        client().create_table(view);
    }

    /**
     * Adds a partition.
     *
     * @param db database
     * @param table table name
     * @param values partition values
     * @param location partition location, or null for the metastore's default below the table
     * @return the partition's location as the metastore stores it
     * @throws TException on metastore errors
     */
    public synchronized String addPartition(String db, String table, List<String> values,
            String location) throws TException {
        return addPartition(db, table, values, location, null);
    }

    /**
     * Adds a partition with serde parameters of its own.
     *
     * @param db database
     * @param table table name
     * @param values partition values
     * @param location partition location, or null for the default
     * @param serdeParameters the partition's serde parameters, or null for the table's; the key
     *     {@link #SERDE_LIB} gives the partition a serde class of its own
     * @return the partition's location as the metastore stores it
     * @throws TException on metastore errors
     */
    public synchronized String addPartition(String db, String table, List<String> values,
            String location, Map<String, String> serdeParameters) throws TException {
        // With capabilities: HMS 3 refuses insert-only tables to clients that do not declare them.
        GetTableRequest request = new GetTableRequest(db, table);
        request.setCapabilities(new ClientCapabilities(
                new ArrayList<>(List.of(ClientCapability.INSERT_ONLY_TABLES))));
        Table t = client().get_table_req(request).getTable();
        StorageDescriptor sd = t.getSd().deepCopy();
        sd.setLocation(location);
        if (serdeParameters != null) {
            Map<String, String> parameters = new HashMap<>(serdeParameters);
            String lib = parameters.remove(SERDE_LIB);
            if (lib != null) {
                sd.getSerdeInfo().setSerializationLib(lib);
            }
            sd.getSerdeInfo().setParameters(parameters);
        }
        Partition partition = new Partition(new ArrayList<>(values), db, table, 0, 0, sd,
                new HashMap<>());
        return client().add_partition(partition).getSd().getLocation();
    }

    /**
     * Drops a partition (not its files).
     *
     * @param db database
     * @param table table name
     * @param values partition values
     * @throws TException on metastore errors
     */
    public synchronized void dropPartition(String db, String table, List<String> values)
            throws TException {
        client().drop_partition(db, table, new ArrayList<>(values), false);
    }

    /**
     * Opens a transaction.
     *
     * @return transaction id
     * @throws TException on metastore errors
     */
    public synchronized long openTxn() throws TException {
        return client().open_txns(new OpenTxnRequest(1, "test", "localhost")).getTxn_ids().get(0);
    }

    /**
     * Allocates a table's next write id to an open transaction.
     *
     * @param txn transaction id
     * @param db database
     * @param table table name
     * @return write id
     * @throws TException on metastore errors
     */
    public synchronized long allocateWriteId(long txn, String db, String table) throws TException {
        AllocateTableWriteIdsRequest request = new AllocateTableWriteIdsRequest(db, table);
        request.setTxnIds(new ArrayList<>(List.of(txn)));
        return client().allocate_table_write_ids(request).getTxnToWriteIds().get(0).getWriteId();
    }

    /**
     * Commits a transaction.
     *
     * @param txn transaction id
     * @throws TException on metastore errors
     */
    public synchronized void commitTxn(long txn) throws TException {
        client().commit_txn(new CommitTxnRequest(txn));
    }

    /**
     * Aborts a transaction.
     *
     * @param txn transaction id
     * @throws TException on metastore errors
     */
    public synchronized void abortTxn(long txn) throws TException {
        client().abort_txn(new AbortTxnRequest(txn));
    }

    /**
     * Starts a TCP proxy in front of the metastore, to cut it off in outage tests.
     *
     * @return the proxy; {@code uri()} names it, {@code close()} cuts every connection
     * @throws IOException when it cannot listen
     */
    public Proxy proxy() throws IOException {
        return new Proxy(port, false);
    }

    /**
     * Starts a TCP proxy that speaks the framed transport to its clients and the unframed one
     * the metastore listens with.
     *
     * @return the proxy; {@code uri()} names it, {@code close()} cuts every connection
     * @throws IOException when it cannot listen
     */
    public Proxy framedProxy() throws IOException {
        return new Proxy(port, true);
    }

    /** A TCP proxy to the metastore. */
    public static final class Proxy implements AutoCloseable {
        private final ServerSocket server;
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();
        private final Object lock = new Object();
        private final int target;
        private final boolean framed;

        Proxy(int target, boolean framed) throws IOException {
            this.target = target;
            this.framed = framed;
            this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread accept = new Thread(this::accept, "hms-proxy");
            accept.setDaemon(true);
            accept.start();
        }

        /**
         * Names the proxy.
         *
         * @return {@code thrift://127.0.0.1:<port>}
         */
        public String uri() {
            return "thrift://127.0.0.1:" + server.getLocalPort();
        }

        private void accept() {
            while (!server.isClosed()) {
                try {
                    Socket in = server.accept();
                    Socket out = new Socket(InetAddress.getLoopbackAddress(), target);
                    synchronized (lock) {
                        sockets.add(in);
                        sockets.add(out);
                        if (server.isClosed()) {
                            // close() ran while this connection was being accepted: cut it too.
                            in.close();
                            out.close();
                            return;
                        }
                    }
                    pump(in, out, framed ? Pump.UNFRAME : Pump.COPY);
                    pump(out, in, framed ? Pump.FRAME : Pump.COPY);
                } catch (IOException e) {
                    return;
                }
            }
        }

        /** How a pump changes the bytes it forwards. */
        private enum Pump {
            /** As they are. */
            COPY,
            /** Each chunk read becomes one frame (a 4-byte length, then the bytes). */
            FRAME,
            /** Frames are read and only their bytes forwarded. */
            UNFRAME
        }

        private void pump(Socket from, Socket to, Pump mode) {
            Thread t = new Thread(() -> {
                byte[] buffer = new byte[65536];
                try (InputStream input = from.getInputStream(); OutputStream output = to.getOutputStream()) {
                    if (mode == Pump.UNFRAME) {
                        DataInputStream frames = new DataInputStream(input);
                        while (true) {
                            byte[] frame = new byte[frames.readInt()];
                            frames.readFully(frame);
                            output.write(frame);
                            output.flush();
                        }
                    }
                    int n;
                    while ((n = input.read(buffer)) >= 0) {
                        if (mode == Pump.FRAME) {
                            output.write(new byte[] {(byte) (n >>> 24), (byte) (n >>> 16),
                                (byte) (n >>> 8), (byte) n});
                        }
                        output.write(buffer, 0, n);
                        output.flush();
                    }
                } catch (EOFException e) {
                    // client closed between frames
                } catch (IOException e) {
                    // connection cut
                } finally {
                    try {
                        to.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }, "hms-proxy-pump");
            t.setDaemon(true);
            t.start();
        }

        @Override
        public void close() throws IOException {
            synchronized (lock) {
                server.close();
                for (Socket socket : sockets) {
                    socket.close();
                }
            }
        }
    }

    private void start() throws Exception {
        Files.createDirectories(dir.resolve("wh"));
        Configuration conf = MetastoreConf.newMetastoreConf();
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY,
                "jdbc:derby:memory:hms" + port + ";create=true");
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE, "file:" + warehouse());
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.SCHEMA_VERIFICATION, false);
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, true);
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
                "org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy");
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TASK_THREADS_ALWAYS,
                "org.apache.hadoop.hive.metastore.events.EventCleanerTask");
        conf.set("datanucleus.schema.autoCreateAll", "true");
        conf.set("hive.metastore.event.db.notification.api.auth", "false");
        // Transaction tables (insert-only tables, write ids), as Hive's own tests create them.
        TxnDbUtil.prepDb(conf);
        Thread server = new Thread(() -> {
            try {
                HiveMetaStore.startMetaStore(port, null, conf);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }, "hms-fixture");
        server.setDaemon(true);
        server.setContextClassLoader(HmsFixture.class.getClassLoader());
        server.start();
        long deadline = System.currentTimeMillis() + 120_000L;
        while (true) {
            try {
                client().get_all_databases();
                return;
            } catch (TException e) {
                transport = null;
                if (System.currentTimeMillis() > deadline || !server.isAlive()) {
                    throw new IllegalStateException("The metastore did not start", e);
                }
                Thread.sleep(250);
            }
        }
    }

    private ThriftHiveMetastore.Client client() throws TException {
        if (transport == null || !transport.isOpen()) {
            TSocket socket = new TSocket("127.0.0.1", port, 60_000);
            socket.open();
            transport = socket;
            client = new ThriftHiveMetastore.Client(new TBinaryProtocol(socket));
        }
        return client;
    }

    private static StorageDescriptor sd(String location, List<FieldSchema> columns, String format) {
        String input;
        String output;
        String serde;
        switch (format) {
            case "parquet" -> {
                input = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
                output = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
                serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
            }
            case "orc" -> {
                input = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
                output = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
                serde = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
            }
            case "iceberg" -> {
                input = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";
                output = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";
                serde = "org.apache.iceberg.mr.hive.HiveIcebergSerDe";
            }
            case "avro" -> {
                input = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
                output = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
                serde = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
            }
            case "csv" -> {
                input = "org.apache.hadoop.mapred.TextInputFormat";
                output = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
                serde = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
            }
            case "json", "json2", "openx" -> {
                input = "org.apache.hadoop.mapred.TextInputFormat";
                output = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
                serde = switch (format) {
                    case "json" -> "org.apache.hive.hcatalog.data.JsonSerDe";
                    case "json2" -> "org.apache.hadoop.hive.serde2.JsonSerDe";
                    default -> "org.openx.data.jsonserde.JsonSerDe";
                };
            }
            default -> {
                input = "org.apache.hadoop.mapred.TextInputFormat";
                output = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
                serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
            }
        }
        return new StorageDescriptor(columns, location, input, output, false, -1,
                new SerDeInfo(null, serde, new HashMap<>()), new ArrayList<>(), new ArrayList<>(),
                new HashMap<>());
    }

    private static List<FieldSchema> fields(String spec) {
        List<FieldSchema> result = new ArrayList<>();
        if (spec == null || spec.isBlank()) {
            return result;
        }
        for (String part : spec.split(";")) {
            String trimmed = part.strip();
            int space = trimmed.indexOf(' ');
            result.add(new FieldSchema(trimmed.substring(0, space), trimmed.substring(space + 1).strip(),
                    null));
        }
        return result;
    }
}
