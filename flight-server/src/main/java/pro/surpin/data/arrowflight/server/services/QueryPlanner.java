package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.sql.impl.FlightSql;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import pro.surpin.data.arrowflight.server.adapters.HostUtils;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.catalog.CatalogFileKey;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.server.catalog.PartitionPruner;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.storage.DataStore;

import pro.surpin.data.arrowflight.server.LogUtil;

/**
 * Plans query execution across cluster nodes.
 * Determines file locations, assigns files to servers based on data locality and load,
 * and creates Flight endpoints for distributed execution.
 */
public final class QueryPlanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryPlanner.class);
    private static final long FILE_PLAN_TTL_NANOS = TimeUnit.SECONDS.toNanos(30);
    /** Backup locations per partial-result endpoint unless configured otherwise. */
    public static final int DEFAULT_BACKUP_LOCATIONS = 2;

    private final ParquetAdapter parquetAdapter;
    private final ClusterService clusterService;
    private final int backupLocations;
    private final int endpointsPerServer;
    private final Map<String, CachedFilePlan> filePlanCache = new ConcurrentHashMap<>();

    /**
     * Stores immutable table file assignments until the inventory refresh deadline.
     *
     * @param files cached file assignments
     * @param expiresAtNanos monotonic expiration deadline
     */
    private record CachedFilePlan(
            Map<String, FileAssignment> files, long expiresAtNanos) {
    }

    /**
     * Contains one validated view of live servers and their loads.
     *
     * @param serverUris live server URIs
     * @param serverLoads current load by live server
     */
    private record ClusterSnapshot(
            Set<String> serverUris, Map<String, Long> serverLoads) {
    }

    /**
     * Creates QueryPlanner with the default number of backup locations.
     *
     * @param parquetAdapter Parquet metadata adapter
     * @param clusterService cluster coordination service
     */
    public QueryPlanner(ParquetAdapter parquetAdapter, ClusterService clusterService) {
        this(parquetAdapter, clusterService, DEFAULT_BACKUP_LOCATIONS);
    }

    /**
     * Creates QueryPlanner.
     *
     * @param parquetAdapter Parquet metadata adapter
     * @param clusterService cluster coordination service
     * @param backupLocations other nodes listed after the planned node of a partial-result
     *        endpoint, which a client tries in order when the planned node cannot be reached;
     *        0 lists the planned node only
     */
    public QueryPlanner(ParquetAdapter parquetAdapter, ClusterService clusterService,
            int backupLocations) {
        this(parquetAdapter, clusterService, backupLocations, 1);
    }

    /**
     * Creates QueryPlanner that may split one node's files of a partial-result query into
     * several endpoints, which a client reads in parallel.
     *
     * @param parquetAdapter Parquet metadata adapter
     * @param clusterService cluster coordination service
     * @param backupLocations other nodes listed after the planned node of a partial-result
     *        endpoint; 0 lists the planned node only
     * @param endpointsPerServer maximum endpoints per node and query, at least 1
     */
    public QueryPlanner(ParquetAdapter parquetAdapter, ClusterService clusterService,
            int backupLocations, int endpointsPerServer) {
        if (backupLocations < 0) {
            throw new IllegalArgumentException("backupLocations must not be negative: " + backupLocations);
        }
        if (endpointsPerServer < 1) {
            throw new IllegalArgumentException(
                    "endpointsPerServer must be at least 1: " + endpointsPerServer);
        }
        this.parquetAdapter = parquetAdapter;
        this.clusterService = clusterService;
        this.backupLocations = backupLocations;
        this.endpointsPerServer = endpointsPerServer;
        try {
            this.clusterService.registerLocalFiles(this.parquetAdapter.localFileInventory());
            this.parquetAdapter.initCatalogReader();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to publish local Parquet inventory", e);
        }
    }

    /**
     * Determines Flight endpoints for a SQL query, distributing files across live servers.
     * Handles JOIN queries only when one node owns every required shard.
     *
     * @param query SQL query
     * @return list of Flight endpoints
     * @throws IOException on file system error
     */
    public List<FlightEndpoint> determineEndpoints(String query)
            throws IOException {
        return plan(query).endpoints();
    }

    /**
     * Plans endpoints and estimates Parquet input statistics for Spark.
     *
     * @param query SQL query
     * @return endpoints with byte and row estimates
     * @throws IOException on file-system or cluster metadata failure
     * @throws IllegalArgumentException if the query needs one node to see all rows (OFFSET,
     *         HAVING, DISTINCT aggregates) and several nodes serve the table
     */
    public QueryPlan plan(String query) throws IOException {
        return plan(query, false);
    }

    /**
     * Plans endpoints and estimates Parquet input statistics, listing backup nodes per endpoint.
     *
     * @param query SQL query
     * @param plannerAsBackup whether this node may be listed as a backup: only for a client that
     *        tries the locations in order. An older client reads an endpoint from the node it
     *        planned with whenever that node is among the locations.
     * @return endpoints with byte and row estimates
     * @throws IOException on file-system or cluster metadata failure
     * @throws IllegalArgumentException if the query needs one node to see all rows and several
     *         nodes serve the table
     */
    public QueryPlan plan(String query, boolean plannerAsBackup) throws IOException {
        DataStore store = parquetAdapter.dataStore();
        // One read scope: every table of the plan comes from the same data version (lakeFS commit).
        try (DataStore.ReadScope scope = store == null ? DataStore.ReadScope.NONE
                : store.readScope()) {
            return planInScope(query, plannerAsBackup);
        }
    }

    private QueryPlan planInScope(String query, boolean plannerAsBackup) throws IOException {
        long t = LogUtil.mark();
        ParquetQueryParser parsed = ParquetQueryParser.parse(query);
        long tSv = LogUtil.mark();
        ClusterSnapshot cluster = validatedCluster();
        Set<String> allServerUris = cluster.serverUris();
        LogUtil.logTiming(tSv, "planning.validateServers", "servers=" + allServerUris.size());
        String singleNodeFeature = parsed.singleNodeFeature();
        if (singleNodeFeature != null && allServerUris.size() > 1) {
            // Every endpoint answers for its own files only; these results cannot be combined
            // by the client afterwards.
            throw new IllegalArgumentException(singleNodeFeature
                    + " is not supported when the table is served by several Flight nodes");
        }
        if (!parsed.isJoin && parquetAdapter.isCatalogSchema(parsed.schema)) {
            return planCatalogTable(query, parsed, cluster, plannerAsBackup);
        }
        int parts = endpointsPerNode(parsed);
        long tPaths = LogUtil.mark();
        Map<String, FileAssignment> pathLocations = validatedPathLocations(parsed, allServerUris);
        LogUtil.logTiming(tPaths, "planning.fileLocations", "files=" + pathLocations.size());

        Map<String, Long> loads = new HashMap<>(cluster.serverLoads());
        Backups backups = new Backups(pathLocations, loads, parquetAdapter.sharedStorage(),
                plannerAsBackup ? null : clusterService.serverUri(), backupLocations);
        List<FlightEndpoint> endpoints;
        if (parquetAdapter.withoutLocality()) {
            // Every node reads every file equally well: balance by bytes, also across queries.
            boolean loadTracked = allServerUris.size() > 1;
            if (parsed.isJoin) {
                endpoints = joinEndpointsWithoutLocality(query, sourceCacheKey(parsed),
                        pathLocations, loads, loadTracked, backups);
            } else {
                endpoints = distributeEndpointsWithoutLocality(query, pathLocations,
                        loads, loadTracked, backups, parts);
            }
        } else {
            boolean loadTracked = hasReplicatedFiles(pathLocations, allServerUris);
            if (parsed.isJoin) {
                endpoints = joinEndpoints(
                        query, pathLocations, allServerUris, loadTracked, backups);
            } else {
                endpoints = distributeEndpoints(query, pathLocations,
                        loads, loadTracked, backups, parts);
            }
        }
        long totalBytes = pathLocations.values().stream()
                .mapToLong(FileAssignment::size).sum();
        long totalRecords = parquetAdapter.estimateRowCount(pathLocations);
        LogUtil.logTiming(t, "planning.determineEndpoints", "endpoints=" + endpoints.size() + " files=" + pathLocations.size());
        return new QueryPlan(endpoints, totalBytes, totalRecords);
    }

    /**
     * Tells why a query asked for in parts gets its complete answer because of the catalog
     * tables it reads (Iceberg): a join of a catalog table, or a snapshot whose data files
     * cannot be read one by one (delete files, nested column changes, no data files).
     *
     * @param query client SQL
     * @return reason, or null when the query reads no catalog table or its files can be split
     */
    public String catalogCompleteReason(String query) {
        CatalogTables catalogTables = parquetAdapter.catalogTables();
        if (catalogTables == null || catalogTables.isEmpty()) {
            return null;
        }
        ParquetQueryParser parsed;
        try {
            parsed = ParquetQueryParser.parse(query);
        } catch (RuntimeException e) {
            return null;   // the partial path reports the parser's error
        }
        if (parsed.isJoin) {
            for (ParquetQueryParser.JoinTable table : parsed.joinTables) {
                if (parquetAdapter.isCatalogSchema(table.schema())) {
                    return "the join reads the catalog table " + table.schema() + "." + table.table();
                }
            }
            return null;
        }
        if (!parquetAdapter.isCatalogSchema(parsed.schema) || parsed.table == null) {
            return null;
        }
        try {
            return parquetAdapter.catalogTables().current(parsed.schema, parsed.table)
                    .map(CatalogTables::completeReason).orElse(null);
        } catch (IOException e) {
            return null;   // planning reports it
        }
    }

    /**
     * Tells whether every table a query reads belongs to a catalog besides the data directory:
     * then a complete answer does not depend on the data directory's files on other nodes.
     *
     * @param query client SQL
     * @return true when the query names only catalog tables; false when it cannot be parsed
     */
    private boolean readsOnlyCatalogTables(String query) {
        CatalogTables catalogTables = parquetAdapter.catalogTables();
        if (catalogTables == null || catalogTables.isEmpty()) {
            return false;
        }
        try {
            ParquetQueryParser parsed = ParquetQueryParser.parse(query);
            if (parsed.isJoin) {
                return parsed.joinTables.stream()
                        .allMatch(t -> parquetAdapter.isCatalogSchema(t.schema()));
            }
            return parquetAdapter.isCatalogSchema(parsed.schema);
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Plans a query of one catalog table in parts: the data files of its current snapshot go
     * to the live nodes like Parquet files without locality, each named with the snapshot ID,
     * so that every node reads the same snapshot. A snapshot whose files cannot be read one by
     * one (it changed after the caller checked) is answered completely instead.
     *
     * @param query client SQL
     * @param parsed parsed query
     * @param cluster live nodes and their loads
     * @param plannerAsBackup whether this node may be listed as a backup
     * @return plan
     * @throws IOException when the table does not exist or its catalog cannot be read
     */
    private QueryPlan planCatalogTable(String query, ParquetQueryParser parsed,
            ClusterSnapshot cluster, boolean plannerAsBackup) throws IOException {
        long t = LogUtil.mark();
        CatalogTables.Resolved resolved = parquetAdapter.catalogTables()
                .current(parsed.schema, parsed.table)
                .orElseThrow(() -> new IOException("No distributed Parquet files found for query: "
                        + "no table " + parsed.schema + "." + parsed.table + " in its catalog"));
        String reason = CatalogTables.completeReason(resolved);
        if (reason != null) {
            LOGGER.info("node={} query=answeredCompletely mode=partial reason='{}' query='{}'",
                    LogUtil.node(), reason, query);
            return planComplete(query, null);
        }
        TableSnapshot snapshot = resolved.snapshot();
        // Parquet files of other catalogs (Hive) travel with their partition values, read once
        // here: executing nodes never list partitions again. Partitions the WHERE clause rules
        // out are left out (one file stays, so that every query has an endpoint).
        boolean shipValues = snapshot.read() instanceof ReadSpec.ParquetFiles;
        List<TableFile> planned = snapshot.files();
        if (shipValues && !snapshot.partitionColumns().isEmpty()) {
            planned = PartitionPruner.prune(snapshot, parsed.filter);
            if (planned.isEmpty()) {
                planned = snapshot.files().subList(0, 1);
            }
            if (planned.size() < snapshot.files().size()) {
                LOGGER.debug("node={} planning=partitionsPruned table={} files={} of {}",
                        LogUtil.node(), snapshot.name(), planned.size(), snapshot.files().size());
            }
        }
        Map<String, FileAssignment> files = new LinkedHashMap<>();
        long records = 0;
        for (TableFile file : planned) {
            CatalogFileKey key = shipValues
                    ? new CatalogFileKey(snapshot.snapshotId(), file.location(), file.partitionValues())
                    : new CatalogFileKey(snapshot.snapshotId(), file.location());
            files.put(key.encode(), new FileAssignment(file.size(), Set.of()));
            records = records < 0 || file.recordCount() < 0 ? -1 : records + file.recordCount();
        }
        Map<String, Long> loads = new HashMap<>(cluster.serverLoads());
        if (!resolved.entry().shared() && loads.size() > 1) {
            // Files on a local disk: only this node is known to read them.
            String self = clusterService.serverUri();
            loads = new HashMap<>(Map.of(self, cluster.serverLoads().getOrDefault(self, 0L)));
        }
        boolean loadTracked = loads.size() > 1;
        Backups backups = new Backups(files, loads, resolved.entry().shared(),
                plannerAsBackup ? null : clusterService.serverUri(), backupLocations);
        List<FlightEndpoint> endpoints = distributeEndpointsWithoutLocality(query, files, loads,
                loadTracked, backups, endpointsPerNode(parsed));
        long totalBytes = files.values().stream().mapToLong(FileAssignment::size).sum();
        LogUtil.logTiming(t, "planning.catalogTable", "table=" + snapshot.name() + " snapshot="
                + snapshot.snapshotId() + " endpoints=" + endpoints.size() + " files=" + files.size());
        return new QueryPlan(endpoints, totalBytes, records);
    }

    /**
     * Plans a query whose complete answer the receiving node computes over all files.
     *
     * @param query client SQL
     * @return one endpoint without a location: the client reads it over the connection it used
     *         to plan the query
     * @throws IllegalStateException when the nodes keep their files apart: the receiving node
     *         cannot read the others' files, so its answer would be incomplete
     */
    public QueryPlan planComplete(String query) {
        return planComplete(query, null);
    }

    /**
     * Plans a query with parameter values whose complete answer the receiving node computes.
     *
     * @param query client SQL
     * @param parameters one-row Arrow IPC stream with the values, or null
     * @return one endpoint without a location
     * @throws IllegalStateException when the nodes keep their files apart
     */
    public QueryPlan planComplete(String query, byte[] parameters) {
        if (!parquetAdapter.sharedStorage() && !readsOnlyCatalogTables(query)) {
            int live = clusterService.filterLiveServers(clusterService.allServerLoads().keySet()).size();
            if (live > 1) {
                throw new IllegalStateException("A complete answer needs storage every node can read"
                        + " (HDFS, S3); this cluster of " + live + " nodes keeps its files on local disks."
                        + " Ask for partial results (header x-flight-result-mode: partial) or set"
                        + " resultMode=partial");
            }
        }
        HandleState state = HandleState.forCompleteAnswer(query, parameters);
        ByteString handle = ByteString.copyFrom(clusterService.createEndpointHandle(state));
        Ticket ticket = new Ticket(Any.pack(FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(handle).build()).toByteArray());
        return new QueryPlan(List.of(new FlightEndpoint(ticket)), -1L, -1L);
    }

    private ClusterSnapshot validatedCluster() throws IOException {
        long t = LogUtil.mark();
        Map<String, Long> registry = clusterService.allServerLoads();
        if (registry.isEmpty()) {
            throw new IOException("Flight server registry is empty");
        }
        Set<String> uris = new LinkedHashSet<>(clusterService.filterLiveServers(registry.keySet()));
        if (uris.isEmpty()) {
            throw new IOException("No live Flight servers are registered");
        }
        Set<String> missing = uris.stream()
                .filter(uri -> !clusterService.hasFileInventory(uri))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (!missing.isEmpty()) {
            throw new IOException("Flight nodes have not published file inventories: " + missing);
        }
        LogUtil.logTiming(t, "planning.validateServers", "live=" + uris.size() + " total=" + registry.size());
        Map<String, Long> serverLoad = new HashMap<>();
        for (String uri : uris) {
            Long load = registry.get(uri);
            serverLoad.put(uri, load != null ? load : 0L);
        }
        return new ClusterSnapshot(Set.copyOf(uris), Map.copyOf(serverLoad));
    }

    private Map<String, FileAssignment> validatedPathLocations(
            ParquetQueryParser parsed, Set<String> allServerUris) throws IOException {
        if (parquetAdapter.withoutLocality()) {
            // The store's own listing is current (and cached by the store); node inventories
            // are empty without locality.
            Map<String, FileAssignment> listed = listedFiles(parsed);
            validatePathLocations(listed, parsed, allServerUris, parquetAdapter.sharedStorage());
            return listed;
        }
        String cacheKey = sourceCacheKey(parsed);
        long now = System.nanoTime();
        CachedFilePlan cached = filePlanCache.get(cacheKey);
        Map<String, FileAssignment> pathLocations;
        boolean reusedCache = cached != null && cached.expiresAtNanos() > now;
        if (reusedCache) {
            pathLocations = cached.files();
        } else {
            pathLocations = loadFilePlan(parsed, cacheKey, now);
        }
        try {
            validatePathLocations(pathLocations, parsed, allServerUris,
                    parquetAdapter.sharedStorage());
        } catch (IOException e) {
            if (!reusedCache) {
                throw e;
            }
            filePlanCache.remove(cacheKey, cached);
            pathLocations = loadFilePlan(parsed, cacheKey, System.nanoTime());
            validatePathLocations(pathLocations, parsed, allServerUris,
                    parquetAdapter.sharedStorage());
        }
        return pathLocations;
    }

    /**
     * Loads and caches file assignments for referenced tables.
     *
     * @param parsed parsed SQL query
     * @param cacheKey normalized source-table key
     * @param now current monotonic time
     * @return immutable file assignments
     * @throws IOException when shared storage cannot be listed
     */
    private Map<String, FileAssignment> loadFilePlan(
            ParquetQueryParser parsed, String cacheKey, long now) throws IOException {
        Map<String, FileAssignment> loaded = filterForQuery(
                clusterService.fileLocations(), parsed);
        if (parquetAdapter.sharedStorage()) {
            loaded = withListedFiles(loaded, listedFiles(parsed), cacheKey);
        }
        Map<String, FileAssignment> immutable = Collections.unmodifiableMap(
                new LinkedHashMap<>(loaded));
        if (!immutable.isEmpty()) {
            filePlanCache.put(cacheKey, new CachedFilePlan(
                    immutable, now + FILE_PLAN_TTL_NANOS));
        }
        return immutable;
    }

    /**
     * Takes the files of a plan on shared storage from the store's listing, with the nodes that
     * hold them from the node inventories. The inventories only tell locality: a node publishes
     * them once at startup, so they miss files written later, and the cluster map that keeps
     * them can lose a dead node's entry (several members lost at once). Planning from the
     * inventories alone then silently left those files out (B5: a survivor of a 3-node cluster
     * answered with half of lineitem). A listed file in no inventory goes to the least loaded
     * live node; an inventory file the listing no longer has is gone from storage.
     *
     * @param inventory files of the query's tables in the node inventories, with their holders
     * @param listed files of the query's tables in the store, in path order
     * @param tables tables of the query, for the log
     * @return listed files with their holders
     */
    static Map<String, FileAssignment> withListedFiles(Map<String, FileAssignment> inventory,
            Map<String, FileAssignment> listed, String tables) {
        // Inventory order first (plans stay as they were while every inventory is there),
        // then the files no inventory names.
        Map<String, FileAssignment> result = new LinkedHashMap<>();
        int gone = 0;
        for (Map.Entry<String, FileAssignment> file : inventory.entrySet()) {
            FileAssignment current = listed.get(file.getKey());
            if (current == null) {
                gone++;
            } else {
                result.put(file.getKey(), new FileAssignment(current.size(), file.getValue().hosts()));
            }
        }
        int withoutHolder = 0;
        for (Map.Entry<String, FileAssignment> file : listed.entrySet()) {
            if (!result.containsKey(file.getKey())) {
                result.put(file.getKey(), new FileAssignment(file.getValue().size(), Set.of()));
                withoutHolder++;
            }
        }
        if (withoutHolder > 0 || gone > 0) {
            LOGGER.info("node={} planning=storageListing tables={} listed={} notInInventories={}"
                    + " inventoryOnly={}", LogUtil.node(), tables, listed.size(), withoutHolder, gone);
        }
        return result;
    }

    /**
     * Lists the files of the tables a query reads from the data store, for storage without
     * locality. Tables are matched by name the way node inventories are matched: in any case,
     * and in every schema when the query names none.
     *
     * @param parsed parsed SQL query
     * @return relative path to size, without hosts, in path order per table
     * @throws IOException on storage access failure
     */
    private Map<String, FileAssignment> listedFiles(ParquetQueryParser parsed) throws IOException {
        Map<String, FileAssignment> result = new LinkedHashMap<>();
        if (parsed.isJoin) {
            for (ParquetQueryParser.JoinTable table : parsed.joinTables) {
                addListedFiles(result, table.schema(), table.table());
            }
        } else {
            addListedFiles(result, parsed.schema, parsed.table);
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Adds the files of every table with a name to a file map.
     *
     * @param result file map to add to
     * @param schema schema name, or null for every schema
     * @param table table name
     * @throws IOException on storage access failure
     */
    private void addListedFiles(Map<String, FileAssignment> result, String schema, String table)
            throws IOException {
        if (table == null) {
            return;
        }
        for (String schemaName : new java.util.TreeSet<>(parquetAdapter.getSchemas(null).keySet())) {
            if (schema != null && !schema.isEmpty() && !schemaName.equalsIgnoreCase(schema)) {
                continue;
            }
            if (parquetAdapter.isCatalogSchema(schemaName)) {
                continue;   // catalog tables have no listed files (and their catalog may be down)
            }
            for (String tableName : new java.util.TreeSet<>(
                    parquetAdapter.getTables(schemaName, null).keySet())) {
                if (tableName.equalsIgnoreCase(table)) {
                    for (pro.surpin.data.arrowflight.spi.storage.DataFile file
                            : parquetAdapter.tableFiles(schemaName, tableName)) {
                        result.putIfAbsent(file.relativePath(),
                                new FileAssignment(file.size(), Set.of()));
                    }
                }
            }
        }
    }

    /**
     * Validates cached assignments against current live cluster membership.
     *
     * @param pathLocations file assignments
     * @param parsed parsed SQL query
     * @param allServerUris live server URIs
     * @param sharedStorage whether every node can read every file
     * @throws IOException when files or required shard owners are unavailable
     */
    private static void validatePathLocations(
            Map<String, FileAssignment> pathLocations,
            ParquetQueryParser parsed, Set<String> allServerUris, boolean sharedStorage)
            throws IOException {
        if (pathLocations.isEmpty()) {
            throw new IOException("No distributed Parquet files found for query: " + parsed);
        }
        if (sharedStorage) {
            // Every node can read every file: a file whose nodes are all down goes to a live
            // node (the least loaded one), so a dead node does not take its tables with it.
            return;
        }
        for (Map.Entry<String, FileAssignment> file : pathLocations.entrySet()) {
            boolean hasLiveOwner = file.getValue().hosts().stream()
                    .anyMatch(allServerUris::contains);
            if (!hasLiveOwner) {
                throw new IOException("No live Flight node owns required shard: " + file.getKey());
            }
        }
        // With a local directory per node every node must hold its share of every table, or
        // part of the table is missing.
        requireShardCoverage(pathLocations, parsed, allServerUris);
    }

    private List<FlightEndpoint> joinEndpoints(String query,
            Map<String, FileAssignment> pathLocations, Set<String> allServerUris,
            boolean loadTracked, Backups backups)
            throws IOException {
        String allFilesServer = findServerWithAllFiles(pathLocations, allServerUris);
        if (allFilesServer == null && parquetAdapter.sharedStorage()) {
            // Every node can read every file: run the join where most of its input is local.
            allFilesServer = serverWithMostLocalBytes(pathLocations, allServerUris);
        }
        if (allFilesServer == null) {
            throw new IOException(
                    "Server-side joins require all input shards on one Flight node; "
                            + "Spark must execute this distributed join");
        }
        long addedBytes = pathLocations.values().stream()
                .mapToLong(FileAssignment::size).sum();
        List<String> files = new ArrayList<>(pathLocations.keySet());
        FlightEndpoint ep = createEndpoint(allFilesServer, files, query, addedBytes, loadTracked,
                backups.forEndpoint(allFilesServer, files));
        if (loadTracked) {
            clusterService.addLoad(allFilesServer, addedBytes);
        }
        return List.of(ep);
    }

    /**
     * Tells how many endpoints one node's files of a query may be split into: one when the
     * query needs every row in one place (OFFSET, HAVING, DISTINCT aggregates), since
     * per-endpoint answers of such a query cannot be combined.
     *
     * @param parsed parsed query
     * @return endpoints per node, at least 1
     */
    private int endpointsPerNode(ParquetQueryParser parsed) {
        return parsed.singleNodeFeature() == null ? endpointsPerServer : 1;
    }

    private List<FlightEndpoint> distributeEndpoints(String query,
            Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoad,
            boolean loadTracked, Backups backups, int parts) {
        Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
        for (Map.Entry<String, FileAssignment> entry : pathLocations.entrySet()) {
            FileAssignment fa = entry.getValue();
            String bestServer = pickServer(fa.hosts(), serverLoad);
            serverToFiles.computeIfAbsent(bestServer, k -> new ArrayList<>()).add(entry.getKey());
            serverLoad.merge(bestServer, fa.size(), Long::sum);
        }

        return splitEndpoints(query, serverToFiles, pathLocations, loadTracked, backups, parts);
    }

    /**
     * Runs a join on one node when every node reads every file equally well: the least loaded
     * one, ties broken by a hash of the joined tables, so that joins spread over the cluster
     * instead of all going to the node with the smallest URI.
     *
     * @param query SQL query
     * @param tablesKey normalized names of the joined tables
     * @param pathLocations files of the joined tables
     * @param serverLoads current load by live server
     * @param loadTracked whether the join's bytes count toward the node's load
     * @param backups chooses the backup nodes of the endpoint
     * @return one endpoint
     */
    private List<FlightEndpoint> joinEndpointsWithoutLocality(String query, String tablesKey,
            Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoads,
            boolean loadTracked, Backups backups) {
        String server = leastLoadedServer(tablesKey, serverLoads);
        long addedBytes = pathLocations.values().stream()
                .mapToLong(FileAssignment::size).sum();
        List<String> files = new ArrayList<>(pathLocations.keySet());
        FlightEndpoint ep = createEndpoint(server, files, query, addedBytes, loadTracked,
                backups.forEndpoint(server, files));
        if (loadTracked) {
            clusterService.addLoad(server, addedBytes);
        }
        return List.of(ep);
    }

    private List<FlightEndpoint> distributeEndpointsWithoutLocality(String query,
            Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoad,
            boolean loadTracked, Backups backups, int parts) {
        Map<String, List<String>> serverToFiles =
                assignWithoutLocality(pathLocations, serverLoad);
        return splitEndpoints(query, serverToFiles, pathLocations, loadTracked, backups, parts);
    }

    /**
     * Creates the endpoints of the nodes' files, splitting each node's files into at most
     * {@code parts} endpoints of similar size, each with its own backup nodes.
     *
     * @param query SQL query
     * @param serverToFiles files by planned node
     * @param pathLocations sizes and holders of the files
     * @param loadTracked whether the endpoints' bytes count toward the nodes' load
     * @param backups chooses the backup nodes of an endpoint
     * @param parts maximum endpoints per node
     * @return endpoints
     */
    private List<FlightEndpoint> splitEndpoints(String query,
            Map<String, List<String>> serverToFiles, Map<String, FileAssignment> pathLocations,
            boolean loadTracked, Backups backups, int parts) {
        List<FlightEndpoint> endpoints = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : serverToFiles.entrySet()) {
            String server = entry.getKey();
            for (List<String> part : splitBySize(entry.getValue(), pathLocations, parts)) {
                long addedBytes = part.stream()
                        .mapToLong(file -> pathLocations.get(file).size()).sum();
                endpoints.add(createEndpoint(server, part, query, addedBytes, loadTracked,
                        backups.forEndpoint(server, part)));
                if (loadTracked) {
                    clusterService.addLoad(server, addedBytes);
                }
            }
        }
        return endpoints;
    }

    /**
     * Splits files into at most {@code parts} groups of similar total size: largest file first,
     * always into the lightest group (ties: by path, so plans are deterministic). Every file is
     * in exactly one group; no group is empty; each group keeps the files' given order.
     *
     * @param files file keys
     * @param pathLocations sizes of the files
     * @param parts maximum number of groups, at least 1
     * @return non-empty groups
     */
    static List<List<String>> splitBySize(List<String> files,
            Map<String, FileAssignment> pathLocations, int parts) {
        int count = Math.min(parts, files.size());
        if (count <= 1) {
            return List.of(files);
        }
        List<String> bySize = new ArrayList<>(files);
        bySize.sort(Comparator.comparingLong((String file) -> pathLocations.get(file).size())
                .reversed().thenComparing(Comparator.naturalOrder()));
        long[] totals = new long[count];
        Map<String, Integer> groupOf = new HashMap<>();
        for (String file : bySize) {
            int lightest = 0;
            for (int i = 1; i < count; i++) {
                if (totals[i] < totals[lightest]) {
                    lightest = i;
                }
            }
            groupOf.put(file, lightest);
            totals[lightest] += pathLocations.get(file).size();
        }
        List<List<String>> groups = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            groups.add(new ArrayList<>());
        }
        for (String file : files) {
            groups.get(groupOf.get(file)).add(file);
        }
        return groups;
    }

    /**
     * Chooses the backup nodes of the endpoints of one plan.
     *
     * @param files files of the plan with their sizes and the nodes that hold them
     * @param serverLoad load by live server, including what the plan assigned so far
     * @param sharedStorage whether every node can read every file
     * @param excluded node never listed as a backup, or null
     * @param limit maximum number of backups per endpoint
     */
    private record Backups(Map<String, FileAssignment> files, Map<String, Long> serverLoad,
            boolean sharedStorage, String excluded, int limit) {

        /**
         * Lists the backup nodes of one endpoint.
         *
         * @param primary planned node
         * @param endpointFiles files of the endpoint
         * @return backup node URIs, best first
         */
        List<String> forEndpoint(String primary, List<String> endpointFiles) {
            return backupServers(primary, endpointFiles, files, serverLoad, sharedStorage,
                    excluded, limit);
        }
    }

    /**
     * Lists the live nodes that can run an endpoint instead of its planned node: on shared
     * storage every other live node, otherwise only nodes that hold every file of the endpoint
     * (replicas), because a node resolves the endpoint's relative paths against its own data
     * directory. Nodes holding more of the endpoint's bytes come first (locality), then the less
     * loaded ones, ties broken by a rendezvous hash of the endpoint's first file and the node.
     *
     * @param primary planned node
     * @param endpointFiles relative paths of the endpoint's files
     * @param files sizes and holders of the plan's files
     * @param serverLoad load by live server
     * @param sharedStorage whether every node can read every file
     * @param excluded node never listed, or null
     * @param limit maximum number of backups; 0 lists none
     * @return backup node URIs, best first
     */
    static List<String> backupServers(String primary, List<String> endpointFiles,
            Map<String, FileAssignment> files, Map<String, Long> serverLoad,
            boolean sharedStorage, String excluded, int limit) {
        if (limit <= 0 || endpointFiles.isEmpty()) {
            return List.of();
        }
        String key = endpointFiles.get(0);
        Map<String, Long> localBytes = new HashMap<>();
        List<String> candidates = new ArrayList<>();
        for (String server : serverLoad.keySet()) {
            if (server.equals(primary) || server.equals(excluded)) {
                continue;
            }
            long local = 0L;
            boolean holdsAll = true;
            for (String path : endpointFiles) {
                FileAssignment file = files.get(path);
                if (file != null && file.hosts().contains(server)) {
                    local += file.size();
                } else {
                    holdsAll = false;
                }
            }
            if (sharedStorage || holdsAll) {
                candidates.add(server);
                localBytes.put(server, local);
            }
        }
        Comparator<String> order = Comparator.<String>comparingLong(localBytes::get).reversed()
                .thenComparingLong(server -> {
                    Long load = serverLoad.get(server);
                    return load == null ? 0L : load;
                })
                .thenComparing(Comparator.<String>comparingLong(
                        server -> rendezvousHash(key, server)).reversed())
                .thenComparing(Comparator.naturalOrder());
        candidates.sort(order);
        return List.copyOf(candidates.subList(0, Math.min(limit, candidates.size())));
    }

    /**
     * Assigns files to nodes when every node reads every file equally well: largest file first,
     * each to the node with the least load so far (the load of running queries plus the files
     * assigned here), ties broken by a rendezvous hash of file and node. A table of one file thus
     * lands on a different node per file name instead of always on the same node.
     *
     * @param pathLocations files to assign
     * @param serverLoad current load by live server; updated with the assigned bytes
     * @return node to its files, in path order per node, nodes in URI order
     */
    static Map<String, List<String>> assignWithoutLocality(
            Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoad) {
        List<Map.Entry<String, FileAssignment>> files = new ArrayList<>(pathLocations.entrySet());
        files.sort(Comparator.<Map.Entry<String, FileAssignment>>comparingLong(
                entry -> entry.getValue().size()).reversed()
                .thenComparing(Map.Entry::getKey));
        Map<String, List<String>> assigned = new java.util.TreeMap<>();
        for (Map.Entry<String, FileAssignment> file : files) {
            String server = leastLoadedServer(file.getKey(), serverLoad);
            assigned.computeIfAbsent(server, k -> new ArrayList<>()).add(file.getKey());
            serverLoad.merge(server, file.getValue().size(), Long::sum);
        }
        for (List<String> serverFiles : assigned.values()) {
            Collections.sort(serverFiles);
        }
        return assigned;
    }

    /**
     * Picks the node with the least load; among equally loaded nodes the one with the highest
     * rendezvous hash for the key, which every planner computes the same way.
     *
     * @param key what is placed: a file path or a set of tables
     * @param serverLoad load by live server
     * @return server URI
     */
    static String leastLoadedServer(String key, Map<String, Long> serverLoad) {
        String best = null;
        long bestLoad = Long.MAX_VALUE;
        long bestHash = 0;
        for (Map.Entry<String, Long> server : serverLoad.entrySet()) {
            long load = server.getValue() == null ? 0L : server.getValue();
            long hash = rendezvousHash(key, server.getKey());
            if (best == null || load < bestLoad || (load == bestLoad
                    && (hash > bestHash || (hash == bestHash && server.getKey().compareTo(best) < 0)))) {
                best = server.getKey();
                bestLoad = load;
                bestHash = hash;
            }
        }
        if (best == null) {
            throw new IllegalStateException("No live Flight servers to assign " + key + " to");
        }
        return best;
    }

    /**
     * Hashes a key with a node, the same on every JVM: 64-bit FNV-1a over the UTF-8 bytes of
     * both, finished with the SplitMix64 mixer.
     *
     * @param key placed key
     * @param server server URI
     * @return hash
     */
    static long rendezvousHash(String key, String server) {
        long hash = 0xcbf29ce484222325L;
        for (byte b : (key + "\n" + server).getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
            hash ^= b & 0xff;
            hash *= 0x100000001b3L;
        }
        hash = (hash ^ (hash >>> 30)) * 0xbf58476d1ce4e5b9L;
        hash = (hash ^ (hash >>> 27)) * 0x94d049bb133111ebL;
        return hash ^ (hash >>> 31);
    }

    /**
     * Builds a stable cache key from the tables referenced by a query.
     *
     * @param parsed parsed SQL query
     * @return normalized source-table key
     */
    private static String sourceCacheKey(ParquetQueryParser parsed) {
        if (!parsed.isJoin) {
            return normalizedTable(parsed.schema, parsed.table);
        }
        return parsed.joinTables.stream()
                .map(table -> normalizedTable(table.schema(), table.table()))
                .sorted()
                .collect(Collectors.joining("|"));
    }

    /**
     * Normalizes a schema and table name for metadata caching.
     *
     * @param schema schema name
     * @param table table name
     * @return normalized qualified table name
     */
    private static String normalizedTable(String schema, String table) {
        return ((schema == null ? "" : schema) + "." + table)
                .toLowerCase(Locale.ROOT);
    }

    /**
     * Checks whether any shard can be assigned to more than one live server.
     *
     * @param files file assignments
     * @param liveServers live server URIs
     * @return true when distributed load tracking can affect placement
     */
    /**
     * Picks the live server that holds the largest share of the given files.
     *
     * @param files file assignments
     * @param liveServers live server URIs
     * @return server URI, or null when there is no live server
     */
    static String serverWithMostLocalBytes(Map<String, FileAssignment> files,
            Set<String> liveServers) {
        Map<String, Long> localBytes = new HashMap<>();
        for (String server : liveServers) {
            localBytes.put(server, 0L);
        }
        for (FileAssignment file : files.values()) {
            for (String host : file.hosts()) {
                localBytes.computeIfPresent(host, (key, bytes) -> bytes + file.size());
            }
        }
        // Most local bytes wins; ties go to the smallest URI so the choice is stable.
        Comparator<Map.Entry<String, Long>> byBytes = Map.Entry.comparingByValue();
        Comparator<Map.Entry<String, Long>> byUri = Map.Entry.comparingByKey();
        return localBytes.entrySet().stream()
                .max(byBytes.thenComparing(byUri.reversed()))
                .map(Map.Entry::getKey).orElse(null);
    }

    private static boolean hasReplicatedFiles(
            Map<String, FileAssignment> files, Set<String> liveServers) {
        return files.values().stream().anyMatch(file ->
                file.hosts().stream().filter(liveServers::contains).limit(2).count() > 1);
    }

    private static Map<String, FileAssignment> filterForQuery(
            Map<String, FileAssignment> inventory, ParquetQueryParser query) {
        Map<String, FileAssignment> result = new LinkedHashMap<>();
        for (Map.Entry<String, FileAssignment> file : inventory.entrySet()) {
            boolean matches;
            if (query.isJoin) {
                matches = query.joinTables.stream().anyMatch(table ->
                        belongsToTable(file.getKey(), table.schema(), table.table()));
            } else {
                matches = belongsToTable(file.getKey(), query.schema, query.table);
            }
            if (matches) {
                result.put(file.getKey(), file.getValue());
            }
        }
        return result;
    }

    private static boolean belongsToTable(String path, String schema, String table) {
        String normalized = path.replace('\\', '/');
        if (schema == null || schema.isEmpty()) {
            String parent = extractTableFromPath(normalized);
            return parent.equalsIgnoreCase(table)
                    || parent.toLowerCase().endsWith("." + table.toLowerCase());
        }
        String prefix = schema + "/" + table + "/";
        return normalized.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    /**
     * The benchmark generator writes one shard (possibly empty) for every table
     * on every configured node. Requiring that coverage prevents an empty or
     * incomplete inventory from being mistaken for a complete distributed table.
     */
    private static void requireShardCoverage(Map<String, FileAssignment> files,
            ParquetQueryParser query, Set<String> liveServers) throws IOException {
        for (String server : liveServers) {
            if (query.isJoin) {
                for (ParquetQueryParser.JoinTable table : query.joinTables) {
                    if (!ownsTableShard(files, server, table.schema(), table.table())) {
                        throw new IOException("Flight node " + server
                                + " has no shard for required table " + table.table());
                    }
                }
            } else if (!ownsTableShard(files, server, query.schema, query.table)) {
                throw new IOException("Flight node " + server
                        + " has no shard for required table " + query.table);
            }
        }
    }

    private static boolean ownsTableShard(Map<String, FileAssignment> files,
            String server, String schema, String table) {
        return files.entrySet().stream().anyMatch(file ->
                file.getValue().hosts().contains(server)
                        && belongsToTable(file.getKey(), schema, table));
    }

    /**
     * Creates a Flight endpoint for a set of files assigned to a server.
     *
     * @param serverUri target server URI
     * @param filePaths file paths to stream
     * @param query     SQL query
     * @param bytes     estimated byte count for load tracking
     * @return Flight endpoint
     */
    public FlightEndpoint createEndpoint(String serverUri, List<String> filePaths,
            String query, long bytes) {
        return createEndpoint(serverUri, filePaths, query, bytes, true, List.of());
    }

    /**
     * Creates a Flight endpoint with explicit distributed load accounting.
     *
     * @param serverUri target server URI
     * @param filePaths file paths to stream
     * @param query SQL query
     * @param bytes estimated byte count
     * @param loadTracked whether bytes were added to distributed server load
     * @param backups other nodes that can run the endpoint, listed after the planned one
     * @return Flight endpoint
     */
    private FlightEndpoint createEndpoint(String serverUri, List<String> filePaths,
            String query, long bytes, boolean loadTracked, List<String> backups) {
        long t = LogUtil.mark();
        // Preserve the registered URI (including grpc+tls). Reconstructing every
        // location as insecure both loses transport information and can cause the
        // client to send a ticket to the wrong connection.
        Location[] locations = new Location[1 + backups.size()];
        locations[0] = new Location(URI.create(serverUri));
        for (int i = 0; i < backups.size(); i++) {
            locations[i + 1] = new Location(URI.create(backups.get(i)));
        }
        HandleState state = HandleState.forServerFiles(
                query, filePaths.toArray(new String[0]),
                serverUri, bytes, loadTracked);
        ByteString serverHandle = ByteString.copyFrom(
                clusterService.createEndpointHandle(state));
        Ticket serverTicket = new Ticket(Any.pack(
                FlightSql.TicketStatementQuery.newBuilder()
                        .setStatementHandle(serverHandle).build())
                .toByteArray());
        LogUtil.logTiming(t, "planning.createEndpoint", "server=" + serverUri + " backups=" + backups
                + " files=" + filePaths.size() + " bytes=" + bytes);
        return new FlightEndpoint(serverTicket, locations);
    }

    /**
     * Picks the best server for a set of file block hosts.
     * Prefers local servers with smallest load; falls back to globally least-loaded.
     *
     * @param fileHosts  set of hosts that have the file blocks
     * @param serverLoad current server loads
     * @return selected server URI
     */
    public static String pickServer(Set<String> fileHosts, Map<String, Long> serverLoad) {
        Set<String> normalizedFileHosts = fileHosts.stream()
                .map(HostUtils::normalize)
                .collect(Collectors.toSet());

        var localServers = serverLoad.keySet().stream()
                .filter(uri -> normalizedFileHosts.contains(HostUtils.normalize(uri)))
                .toList();

        boolean hasLocality = !localServers.isEmpty() && localServers.size() < serverLoad.size();
        var candidates = hasLocality ? localServers : List.copyOf(serverLoad.keySet());
        return candidates.stream()
                .min(Comparator.comparingLong(serverLoad::get))
                .orElseThrow();
    }

    /**
     * Groups files by their assigned server based on data locality and load.
     *
     * @param pathLocations file to host assignments
     * @param serverLoad    current server loads
     * @return map of server URI to list of file paths
     */
    public static Map<String, List<String>> groupFilesByServer(
            Map<String, FileAssignment> pathLocations, Map<String, Long> serverLoad) {
        Map<String, List<String>> serverToFiles = new LinkedHashMap<>();
        for (Map.Entry<String, FileAssignment> entry : pathLocations.entrySet()) {
            String serverUri = pickServer(entry.getValue().hosts(), serverLoad);
            serverToFiles.computeIfAbsent(serverUri, k -> new ArrayList<>()).add(entry.getKey());
        }
        return serverToFiles;
    }

    /**
     * Finds a server that has ALL files in the map, or null if none.
     *
     * @param pathLocations file to host assignments
     * @param allServerUris all registered server URIs
     * @return server URI with all files, or null
     */
    private String findServerWithAllFiles(
            Map<String, FileAssignment> pathLocations, Set<String> allServerUris) {
        outer:
        for (String serverUri : allServerUris) {
            String normServer = HostUtils.normalize(serverUri);
            for (FileAssignment fa : pathLocations.values()) {
                boolean hasHost = false;
                for (String host : fa.hosts()) {
                    if (HostUtils.normalize(host).equals(normServer)) {
                        hasHost = true;
                        break;
                    }
                }
                if (!hasHost) {
                    continue outer;
                }
            }
            return serverUri;
        }
        return null;
    }

    /**
     * Extracts the table name from a relative file path.
     *
     * @param path relative file path
     * @return table name derived from parent directory
     */
    public static String extractTableFromPath(String path) {
        int lastSep = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        if (lastSep < 0) {
            return path;
        }
        String parent = path.substring(0, lastSep);
        return parent.replace('\\', '.').replace('/', '.');
    }
}
