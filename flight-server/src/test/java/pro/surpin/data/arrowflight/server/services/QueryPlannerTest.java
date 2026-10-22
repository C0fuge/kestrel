package pro.surpin.data.arrowflight.server.services;

import pro.surpin.data.arrowflight.server.adapters.HostUtils;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import org.apache.arrow.flight.FlightEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryPlannerTest {

    // ── pickServer ──────────────────────────────────────────────────────────
    //
    // HostUtils.normalize() resolves hostnames via InetAddress.getByName().
    // Without real DNS, arbitrary names resolve to themselves. To test locality
    // matching, file hosts and server URIs must use names that normalize
    // identically — both "127.0.0.1" resolves to "127.0.0.1" regardless of form.

    private static final String S1 = "grpc://127.0.0.1:32010";
    private static final String S2 = "grpc://127.0.0.2:32010";
    private static final String H1 = "127.0.0.1";
    private static final String H2 = "127.0.0.2";

    @Test
    void pickServerPrefersLocal() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 100L);
        load.put(S2, 50L);

        // File blocks on H1 — prefer S1 (has locality)
        String server = QueryPlanner.pickServer(Set.of(H1), load);
        assertEquals(S1, server);
    }

    @Test
    void pickServerFallsBackToGlobalWhenNoLocality() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 200L);
        load.put(S2, 50L);

        // File blocks on unknown host — fallback to least-loaded
        String server = QueryPlanner.pickServer(Set.of("unknown-host"), load);
        assertEquals(S2, server);
    }

    @Test
    void pickServerLeastLoadedAmongMultipleLocals() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 500L);
        load.put(S2, 0L);

        // File blocks on BOTH S1 and S2 — pick lower load
        String server = QueryPlanner.pickServer(Set.of(H1, H2), load);
        assertEquals(S2, server);
    }

    @Test
    void pickServerSingleServer() {
        Map<String, Long> load = Map.of(S1, 0L);
        String server = QueryPlanner.pickServer(Set.of("x"), load);
        assertEquals(S1, server);
    }

    @Test
    void pickServerThrowsOnEmptyLoad() {
        assertThrows(Exception.class, () ->
                QueryPlanner.pickServer(Set.of(H1), Map.of()));
    }

    // ── groupFilesByServer ──────────────────────────────────────────────────

    @Test
    void groupFilesByServerGroupsAllToSingleServer() {
        Map<String, FileAssignment> locations = new LinkedHashMap<>();
        locations.put("f1.parquet", new FileAssignment(100, Set.of(H1)));
        locations.put("f2.parquet", new FileAssignment(200, Set.of(H1)));

        Map<String, Long> load = Map.of(S1, 0L);

        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(locations, load);

        assertEquals(1, result.size());
        assertEquals(2, result.get(S1).size());
    }

    @Test
    void groupFilesByServerDistributesByLocality() {
        Map<String, FileAssignment> locations = new LinkedHashMap<>();
        locations.put("f1.parquet", new FileAssignment(100, Set.of(H1)));
        locations.put("f2.parquet", new FileAssignment(200, Set.of(H2)));

        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 0L);
        load.put(S2, 0L);

        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(locations, load);

        assertEquals(1, result.get(S1).size());
        assertEquals(1, result.get(S2).size());
    }

    @Test
    void groupFilesByServerEmptyInput() {
        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(
                Map.of(), Map.of(S1, 0L));

        assertTrue(result.isEmpty());
    }

    // ── extractTableFromPath ────────────────────────────────────────────────

    @Test
    void extractTableFromUnixPath() {
        assertEquals(".data.db.table", QueryPlanner.extractTableFromPath("/data/db/table/file.parquet"));
    }

    @Test
    void extractTableFromPathSameSchemaSameTable() {
        String t1 = QueryPlanner.extractTableFromPath("/data/s1/t1/file.parquet");
        String t2 = QueryPlanner.extractTableFromPath("/data/s1/t1/other.parquet");

        assertEquals(t1, t2);
    }

    @Test
    void extractTableFromPathDifferentTables() {
        String t1 = QueryPlanner.extractTableFromPath("/data/s1/table_a/f.parquet");
        String t2 = QueryPlanner.extractTableFromPath("/data/s2/table_b/f.parquet");

        assertNotEquals(t1, t2);
    }

    // ── filterForQuery (private static, via reflection) ────────────────────

    @Test
    void filterForQueryKeepsMatchingTableFiles() throws Exception {
        Map<String, FileAssignment> inventory = new LinkedHashMap<>();
        inventory.put("s/t/f1.parquet", new FileAssignment(100, Set.of("H1")));
        inventory.put("other/f2.parquet", new FileAssignment(200, Set.of("H2")));

        ParquetQueryParser pq = ParquetQueryParser.parse("SELECT * FROM s.t");
        Map<String, FileAssignment> result = invokeFilterForQuery(inventory, pq);

        assertEquals(1, result.size());
        assertTrue(result.containsKey("s/t/f1.parquet"));
    }

    @Test
    void filterForQueryExcludesUnrelatedFiles() throws Exception {
        Map<String, FileAssignment> inventory = new LinkedHashMap<>();
        inventory.put("x/y/f.parquet", new FileAssignment(100, Set.of("H1")));

        ParquetQueryParser pq = ParquetQueryParser.parse("SELECT * FROM s.t");
        Map<String, FileAssignment> result = invokeFilterForQuery(inventory, pq);

        assertTrue(result.isEmpty());
    }

    // ── belongsToTable (private static, via reflection) ────────────────────

    @Test
    void belongsToTableWithSchemaMatchesPrefix() throws Exception {
        assertTrue(invokeBelongsToTable("s/t/f.parquet", "s", "t"));
        assertTrue(invokeBelongsToTable("s/t/sub/f.parquet", "s", "t"));
        assertFalse(invokeBelongsToTable("other/t/f.parquet", "s", "t"));
    }

    @Test
    void belongsToTableWithoutSchemaMatchesExtractedTable() throws Exception {
        assertTrue(invokeBelongsToTable("/data/p/q/f.parquet", null, "q"),
                "last segment should match table name");
        assertFalse(invokeBelongsToTable("/data/p/q/f.parquet", null, "x"),
                "unrelated table should not match");
    }

    @Test
    void belongsToTableCaseInsensitive() throws Exception {
        assertTrue(invokeBelongsToTable("Schema/Table/f.parquet", "Schema", "Table"));
    }

    // ── ownsTableShard (private static, via reflection) ────────────────────

    @Test
    void ownsTableShardTrueWhenOwnerAndMatch() throws Exception {
        Map<String, FileAssignment> files = Map.of(
                "s/t/f.parquet", new FileAssignment(100, Set.of("H1")));
        assertTrue(invokeOwnsTableShard(files, "H1", "s", "t"));
    }

    @Test
    void ownsTableShardFalseWhenWrongOwner() throws Exception {
        Map<String, FileAssignment> files = Map.of(
                "s/t/f.parquet", new FileAssignment(100, Set.of("H1")));
        assertFalse(invokeOwnsTableShard(files, "H2", "s", "t"));
    }

    @Test
    void ownsTableShardFalseWhenWrongTable() throws Exception {
        Map<String, FileAssignment> files = Map.of(
                "s/t/f.parquet", new FileAssignment(100, Set.of("H1")));
        assertFalse(invokeOwnsTableShard(files, "H1", "s", "other"));
    }

    // ── createEndpoint (public, needs ClusterService mock) ─────────────────

    @Mock
    private ClusterService clusterService;

    @Mock
    private ParquetAdapter parquetAdapter;

    @Test
    void createEndpointReturnsValidTicket() throws Exception {
        when(parquetAdapter.localFileInventory()).thenReturn(Map.of());
        when(clusterService.createEndpointHandle(any()))
                .thenReturn(new byte[]{1, 2, 3});
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);
        FlightEndpoint ep = planner.createEndpoint(
                "grpc://127.0.0.1:32010", List.of("s/t/f.parquet"), "SELECT * FROM s.t", 100L);

        assertNotNull(ep);
        assertNotNull(ep.getTicket());
        assertEquals(1, ep.getLocations().size());
        assertEquals("grpc://127.0.0.1:32010",
                ep.getLocations().get(0).getUri().toString());
        verify(clusterService).createEndpointHandle(any());
    }

    /** Verifies repeated scans reuse static file assignments and report statistics. */
    @Test
    void planCachesFilesAndReportsParquetStatistics() throws Exception {
        when(parquetAdapter.localFileInventory()).thenReturn(Map.of());
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1));
        when(clusterService.hasFileInventory(S1)).thenReturn(true);
        Map<String, FileAssignment> files = Map.of(
                "s/t/f.parquet", new FileAssignment(100L, Set.of(S1)));
        when(clusterService.fileLocations()).thenReturn(files);
        when(clusterService.createEndpointHandle(any()))
                .thenReturn(new byte[]{1, 2, 3});
        when(parquetAdapter.estimateRowCount(anyMap())).thenReturn(25L);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        QueryPlan first = planner.plan("SELECT * FROM s.t");
        QueryPlan second = planner.plan("SELECT id FROM s.t");

        assertEquals(100L, first.totalBytes());
        assertEquals(25L, first.totalRecords());
        assertEquals(1, second.endpoints().size());
        verify(clusterService, times(1)).fileLocations();
        verify(clusterService, never()).addLoad(anyString(), anyLong());
    }

    @Test
    void determineEndpoints_throwsWhenRegistryEmpty() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of());

        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);
        assertThrows(Exception.class, () ->
                planner.determineEndpoints("SELECT * FROM s.t"));
    }

    @Test
    void determineEndpoints_throwsWhenNoLiveServers() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of("s1", 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of());

        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);
        assertThrows(Exception.class, () ->
                planner.determineEndpoints("SELECT * FROM s.t"));
    }

    @Test
    void determineEndpoints_throwsWhenNoInventories() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of("s1", 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of("s1"));
        when(clusterService.hasFileInventory("s1")).thenReturn(false);

        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);
        assertThrows(Exception.class, () ->
                planner.determineEndpoints("SELECT * FROM s.t"));
    }

    // ── backup locations ───────────────────────────────────────────────────

    private static final String N1 = "grpc+tcp://n1:32010";
    private static final String N2 = "grpc+tcp://n2:32010";
    private static final String N3 = "grpc+tcp://n3:32010";
    private static final String N4 = "grpc+tcp://n4:32010";

    private static Map<String, Long> loads(Object... serverAndLoad) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (int i = 0; i < serverAndLoad.length; i += 2) {
            result.put((String) serverAndLoad[i], ((Number) serverAndLoad[i + 1]).longValue());
        }
        return result;
    }

    /** Verifies shared storage lists the other nodes, least loaded first, up to the limit. */
    @Test
    void backupServersOnSharedStorageAreTheLeastLoadedOtherNodes() {
        Map<String, FileAssignment> files = Map.of("s/t/a.parquet", new FileAssignment(100L, Set.of()));
        Map<String, Long> load = loads(N1, 0, N2, 300, N3, 100, N4, 200);

        assertEquals(List.of(N3, N4), QueryPlanner.backupServers(
                N1, List.of("s/t/a.parquet"), files, load, true, null, 2));
        assertEquals(List.of(N3, N4, N2), QueryPlanner.backupServers(
                N1, List.of("s/t/a.parquet"), files, load, true, null, 10));
        assertEquals(List.of(), QueryPlanner.backupServers(
                N1, List.of("s/t/a.parquet"), files, load, true, null, 0));
        assertEquals(List.of(), QueryPlanner.backupServers(
                N1, List.of("s/t/a.parquet"), files, loads(N1, 0), true, null, 2));
    }

    /** Verifies equally loaded backups follow the rendezvous hash, whatever the map order. */
    @Test
    void backupServersBreakTiesByRendezvousHash() {
        Map<String, FileAssignment> files = Map.of("s/t/a.parquet", new FileAssignment(1L, Set.of()));
        List<String> forward = QueryPlanner.backupServers(N1, List.of("s/t/a.parquet"), files,
                loads(N1, 0, N2, 0, N3, 0, N4, 0), true, null, 3);
        List<String> reverse = QueryPlanner.backupServers(N1, List.of("s/t/a.parquet"), files,
                loads(N4, 0, N3, 0, N2, 0, N1, 0), true, null, 3);

        assertEquals(forward, reverse);
        List<String> byHash = new ArrayList<>(List.of(N2, N3, N4));
        byHash.sort(Comparator.comparingLong(
                (String server) -> QueryPlanner.rendezvousHash("s/t/a.parquet", server)).reversed());
        assertEquals(byHash, forward);
    }

    /** Verifies HDFS backups prefer nodes holding replicas of the endpoint's files. */
    @Test
    void backupServersPreferNodesWithLocalReplicas() {
        Map<String, FileAssignment> files = Map.of(
                "s/t/a.parquet", new FileAssignment(100L, Set.of(N1, N3)),
                "s/t/b.parquet", new FileAssignment(50L, Set.of(N1, N2)));

        assertEquals(List.of(N3, N2), QueryPlanner.backupServers(N1,
                List.of("s/t/a.parquet", "s/t/b.parquet"), files,
                loads(N1, 0, N2, 0, N3, 900, N4, 0), true, null, 2));
    }

    /** Verifies local disks only list nodes holding every file of the endpoint. */
    @Test
    void backupServersOnLocalDisksNeedEveryFile() {
        Map<String, FileAssignment> files = Map.of(
                "s/t/a.parquet", new FileAssignment(100L, Set.of(N1, N2, N3)),
                "s/t/b.parquet", new FileAssignment(50L, Set.of(N1, N2)));

        assertEquals(List.of(N2), QueryPlanner.backupServers(N1,
                List.of("s/t/a.parquet", "s/t/b.parquet"), files,
                loads(N1, 0, N2, 0, N3, 0), false, null, 2));
        assertEquals(List.of(N2, N3), QueryPlanner.backupServers(N1,
                List.of("s/t/a.parquet"), files, loads(N1, 0, N2, 0, N3, 10), false, null, 2));
    }

    /** Verifies the excluded node (the planning node, for older clients) is never listed. */
    @Test
    void backupServersSkipTheExcludedNode() {
        Map<String, FileAssignment> files = Map.of("s/t/a.parquet", new FileAssignment(1L, Set.of()));

        assertEquals(List.of(N3), QueryPlanner.backupServers(N1, List.of("s/t/a.parquet"), files,
                loads(N1, 0, N2, 0, N3, 0), true, N2, 2));
    }

    /** Verifies a negative number of backups is refused. */
    @Test
    void negativeBackupLocationsAreRefused() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryPlanner(parquetAdapter, clusterService, -1));
    }

    private static final String P1 = "grpc+tcp://127.0.0.1:32010";
    private static final String P2 = "grpc+tcp://127.0.0.2:32010";
    private static final String P3 = "grpc+tcp://127.0.0.3:32010";

    private static List<String> locations(FlightEndpoint endpoint) {
        return endpoint.getLocations().stream().map(l -> l.getUri().toString()).toList();
    }

    /** Verifies shared-storage endpoints list backups after the planned node, the planner only on request. */
    @Test
    void planListsBackupLocationsAfterThePlannedNode() throws Exception {
        when(clusterService.serverUri()).thenReturn(P1);
        when(clusterService.allServerLoads()).thenReturn(Map.of(P1, 0L, P2, 0L, P3, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(P1, P2, P3));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.fileLocations()).thenReturn(Map.of(
                "s/t/a.parquet", new FileAssignment(100L, Set.of(P2)),
                "s/t/b.parquet", new FileAssignment(100L, Set.of(P3))));
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.sharedStorage()).thenReturn(true);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        Map<String, List<String>> byPrimary = new HashMap<>();
        for (FlightEndpoint endpoint : planner.plan("SELECT * FROM s.t").endpoints()) {
            byPrimary.put(locations(endpoint).get(0), locations(endpoint));
        }
        // Without the client's consent the planning node P1 is never a backup: an older client
        // would read every endpoint listing P1 from P1.
        assertEquals(List.of(P2, P3), byPrimary.get(P2));
        assertEquals(List.of(P3, P2), byPrimary.get(P3));

        for (FlightEndpoint endpoint : planner.plan("SELECT * FROM s.t", true).endpoints()) {
            List<String> locations = locations(endpoint);
            assertEquals(3, locations.size());
            assertEquals(Set.of(P1, P2, P3), Set.copyOf(locations));
        }

        // Complete answers are unchanged: one endpoint without a location.
        assertTrue(planner.planComplete("SELECT 1").endpoints().get(0).getLocations().isEmpty());
    }

    /** Verifies 0 backup locations gives the planned node only. */
    @Test
    void zeroBackupLocationsKeepOneLocation() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of(P1, 0L, P2, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(P1, P2));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.fileLocations()).thenReturn(Map.of(
                "s/t/a.parquet", new FileAssignment(100L, Set.of(P2))));
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.sharedStorage()).thenReturn(true);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService, 0);

        QueryPlan plan = planner.plan("SELECT * FROM s.t", true);

        assertEquals(List.of(List.of(P2)), plan.endpoints().stream().map(QueryPlannerTest::locations).toList());
    }

    /** Verifies local disks list a backup only where the endpoint's files are replicated. */
    @Test
    void planOnLocalDisksListsOnlyReplicaHolders() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of(P1, 0L, P2, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(P1, P2));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.fileLocations()).thenReturn(Map.of(
                "s/t/a.parquet", new FileAssignment(100L, Set.of(P1, P2)),
                "s/t/b.parquet", new FileAssignment(500L, Set.of(P2))));
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.sharedStorage()).thenReturn(false);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        QueryPlan plan = planner.plan("SELECT * FROM s.t", true);

        assertFalse(plan.endpoints().isEmpty());
        for (FlightEndpoint endpoint : plan.endpoints()) {
            List<String> locations = locations(endpoint);
            if (locations.get(0).equals(P2)) {
                // P2 holds b, which P1 does not have.
                assertEquals(List.of(P2), locations);
            } else {
                assertEquals(List.of(P1, P2), locations);
            }
        }
    }

    // ── Reflection helpers ─────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static Map<String, FileAssignment> invokeFilterForQuery(
            Map<String, FileAssignment> inventory, ParquetQueryParser query) throws Exception {
        Method m = QueryPlanner.class.getDeclaredMethod("filterForQuery",
                Map.class, ParquetQueryParser.class);
        m.setAccessible(true);
        return (Map<String, FileAssignment>) m.invoke(null, inventory, query);
    }

    private static boolean invokeBelongsToTable(String path, String schema, String table)
            throws Exception {
        Method m = QueryPlanner.class.getDeclaredMethod("belongsToTable",
                String.class, String.class, String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, path, schema, table);
    }

    private static boolean invokeOwnsTableShard(Map<String, FileAssignment> files,
            String server, String schema, String table) throws Exception {
        Method m = QueryPlanner.class.getDeclaredMethod("ownsTableShard",
                Map.class, String.class, String.class, String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, files, server, schema, table);
    }

    @Test
    void filterForQuery_matchesBySchemaTable() throws Exception {
        Map<String, FileAssignment> inventory = new LinkedHashMap<>();
        inventory.put("s/t/f1.parquet", new FileAssignment(100L, Set.of("H1")));
        inventory.put("other/f.parquet", new FileAssignment(200L, Set.of("H1")));

        Map<String, FileAssignment> result = invokeFilterForQuery(inventory, false, "s", "t");
        assertEquals(1, result.size());
        assertTrue(result.containsKey("s/t/f1.parquet"));
    }

    @Test
    void filterForJoin_matchesMultipleTables() throws Exception {
        Map<String, FileAssignment> inventory = new LinkedHashMap<>();
        inventory.put("s/a/f.parquet", new FileAssignment(100L, Set.of("H1")));
        inventory.put("s/b/f.parquet", new FileAssignment(200L, Set.of("H1")));

        Map<String, FileAssignment> result = invokeFilterForQuery(inventory, true, "s", null);
        assertEquals(2, result.size());
    }

    private static Map<String, FileAssignment> invokeFilterForQuery(
            Map<String, FileAssignment> inventory, boolean isJoin,
            String schema, String table) throws Exception {
        Method m = QueryPlanner.class.getDeclaredMethod("filterForQuery",
                Map.class, ParquetQueryParser.class);
        m.setAccessible(true);
        ParquetQueryParser pq;
        if (isJoin) {
            pq = ParquetQueryParser.parse(
                    "SELECT * FROM " + schema + ".a JOIN " + schema + ".b ON a.id = b.id");
        } else {
            pq = ParquetQueryParser.parse("SELECT * FROM " + schema + "." + table);
        }
        @SuppressWarnings("unchecked")
        Map<String, FileAssignment> r = (Map<String, FileAssignment>) m.invoke(null, inventory, pq);
        return r;
    }

    /** Verifies OFFSET is refused when several nodes would each skip rows on their own. */
    @Test
    void offsetNeedsSingleNode() {
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        assertThrows(IllegalArgumentException.class,
                () -> planner.plan("SELECT * FROM s.t ORDER BY id LIMIT 5 OFFSET 2"));
    }

    /** Verifies a one-file table on shared storage is planned on a cluster. */
    @Test
    void sharedStoragePlansTableHeldByOneNode() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.fileLocations()).thenReturn(Map.of(
                "s/small/f.parquet", new FileAssignment(10L, Set.of(S1))));
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.sharedStorage()).thenReturn(true);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        QueryPlan plan = planner.plan("SELECT * FROM s.small");

        assertEquals(1, plan.endpoints().size());
        assertEquals(S1, plan.endpoints().get(0).getLocations().get(0).getUri().toString());
    }

    /** Verifies the shard check still guards a local directory per node. */
    @Test
    void localStorageStillNeedsEveryNodesShard() {
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.fileLocations()).thenReturn(Map.of(
                "s/small/f.parquet", new FileAssignment(10L, Set.of(S1))));
        when(parquetAdapter.sharedStorage()).thenReturn(false);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        assertThrows(java.io.IOException.class, () -> planner.plan("SELECT * FROM s.small"));
    }

    /** Verifies a join on shared storage runs where most of its input is local. */
    @Test
    void sharedStorageRunsJoinOnNodeWithMostInput() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.fileLocations()).thenReturn(Map.of(
                "s/a/f.parquet", new FileAssignment(10L, Set.of(S1)),
                "s/b/f.parquet", new FileAssignment(500L, Set.of(S2))));
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.sharedStorage()).thenReturn(true);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        QueryPlan plan = planner.plan("SELECT x.id FROM s.a x JOIN s.b y ON x.id = y.id");

        assertEquals(1, plan.endpoints().size());
        assertEquals(S2, plan.endpoints().get(0).getLocations().get(0).getUri().toString());
    }

    /** Verifies a complete answer is refused on a cluster whose nodes keep their files apart. */
    @Test
    void completeAnswerNeedsSharedStorageOnACluster() {
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        when(parquetAdapter.sharedStorage()).thenReturn(false);
        assertThrows(IllegalStateException.class, () -> planner.planComplete("SELECT 1"));

        when(parquetAdapter.sharedStorage()).thenReturn(true);
        assertEquals(1, planner.planComplete("SELECT 1").endpoints().size());

        when(parquetAdapter.sharedStorage()).thenReturn(false);
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1));
        assertEquals(1, planner.planComplete("SELECT 1").endpoints().size());
    }

    /** Verifies files of a dead node go to a live node on shared storage and fail on local disks. */
    @Test
    void filesOfADeadNodeGoToALiveNodeOnSharedStorage() throws Exception {
        when(parquetAdapter.localFileInventory()).thenReturn(Map.of());
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1));
        when(clusterService.hasFileInventory(S1)).thenReturn(true);
        when(clusterService.fileLocations()).thenReturn(Map.of(
                "s/t/a.parquet", new FileAssignment(100L, Set.of(S1)),
                "s/t/b.parquet", new FileAssignment(100L, Set.of(S2))));
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.estimateRowCount(anyMap())).thenReturn(2L);

        when(parquetAdapter.sharedStorage()).thenReturn(true);
        QueryPlan plan = new QueryPlanner(parquetAdapter, clusterService).plan("SELECT * FROM s.t");
        assertEquals(1, plan.endpoints().size());
        assertEquals(200L, plan.totalBytes());

        when(parquetAdapter.sharedStorage()).thenReturn(false);
        assertThrows(IOException.class,
                () -> new QueryPlanner(parquetAdapter, clusterService).plan("SELECT * FROM s.t"));
    }

    // ── no locality (object stores) ─────────────────────────────────────────

    private static final String S3 = "grpc://127.0.0.3:32010";

    /** Verifies files go largest first to the least loaded node. */
    @Test
    void withoutLocalityLargestFileGoesToLeastLoadedNode() {
        Map<String, FileAssignment> files = new LinkedHashMap<>();
        files.put("s/t/small.parquet", new FileAssignment(10, Set.of()));
        files.put("s/t/big.parquet", new FileAssignment(1000, Set.of()));
        files.put("s/t/mid.parquet", new FileAssignment(600, Set.of()));
        files.put("s/t/mid2.parquet", new FileAssignment(500, Set.of()));
        Map<String, Long> load = new HashMap<>(Map.of(S1, 0L, S2, 300L));

        Map<String, List<String>> assigned = QueryPlanner.assignWithoutLocality(files, load);

        // big -> S1 (0); mid -> S2 (300 < 1000); mid2 -> S2 (900 < 1000); small -> S1 (1000 < 1400).
        assertEquals(List.of("s/t/big.parquet", "s/t/small.parquet"), assigned.get(S1));
        assertEquals(List.of("s/t/mid.parquet", "s/t/mid2.parquet"), assigned.get(S2));
        assertEquals(1010L, load.get(S1));
        assertEquals(1400L, load.get(S2));
    }

    /** Verifies equally loaded nodes are chosen by a rendezvous hash, so one-file tables spread. */
    @Test
    void withoutLocalityTiesSpreadSingleFileTables() {
        Map<String, Long> idle = Map.of(S1, 0L, S2, 0L, S3, 0L);
        Set<String> chosen = new HashSet<>();
        for (String table : List.of("region", "nation", "supplier", "customer", "part", "orders",
                "partsupp", "lineitem", "t1", "t2", "t3", "t4")) {
            String file = "tpch/" + table + "/data_0.parquet";
            String server = QueryPlanner.leastLoadedServer(file, idle);
            assertEquals(server, QueryPlanner.leastLoadedServer(file, new TreeMap<>(idle)),
                    "same choice whatever the map order");
            chosen.add(server);
        }

        assertEquals(Set.of(S1, S2, S3), chosen);
        assertEquals(S2, QueryPlanner.leastLoadedServer("x", Map.of(S1, 5L, S2, 1L, S3, 5L)));
    }

    /** Verifies the hash does not depend on the JVM. */
    @Test
    void rendezvousHashIsStable() {
        assertEquals(QueryPlanner.rendezvousHash("tpch/region/data_0.parquet", S1),
                QueryPlanner.rendezvousHash("tpch/region/data_0.parquet", S1));
        assertNotEquals(QueryPlanner.rendezvousHash("a", S1), QueryPlanner.rendezvousHash("a", S2));
        assertNotEquals(QueryPlanner.rendezvousHash("ab", "c"), QueryPlanner.rendezvousHash("a", "bc"));
    }

    /** Verifies planning reads the store's listing, not node inventories, and tracks load. */
    @Test
    void withoutLocalityPlansFromTheListingAndTracksLoad() throws Exception {
        stubListing();
        // S2 is busier by one byte: the larger file goes to S1, the other to S2.
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 1L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.sharedStorage()).thenReturn(true);
        when(parquetAdapter.estimateRowCount(anyMap())).thenReturn(7L);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        QueryPlan plan = planner.plan("SELECT * FROM TPCH.LINEITEM");

        assertEquals(2, plan.endpoints().size());
        assertEquals(700L, plan.totalBytes());
        verify(clusterService, never()).fileLocations();
        verify(clusterService).addLoad(S1, 400L);
        verify(clusterService).addLoad(S2, 300L);
        verify(parquetAdapter).tableFiles("tpch", "lineitem");
    }

    /** Verifies a single node gets everything without load tracking. */
    @Test
    void withoutLocalityOneNodeDoesNotTrackLoad() throws Exception {
        stubListing();
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.sharedStorage()).thenReturn(true);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        QueryPlan plan = planner.plan("SELECT * FROM lineitem");

        assertEquals(1, plan.endpoints().size());
        verify(clusterService, never()).addLoad(anyString(), anyLong());
    }

    /** Verifies joins go to the least loaded node, ties spread by the joined tables. */
    @Test
    void withoutLocalityJoinsSpread() throws Exception {
        stubListing();
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 50L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        when(clusterService.createEndpointHandle(any())).thenReturn(new byte[]{1});
        when(parquetAdapter.sharedStorage()).thenReturn(true);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        QueryPlan plan = planner.plan(
                "SELECT l.a FROM tpch.lineitem l JOIN tpch.nation n ON l.a = n.a");

        assertEquals(1, plan.endpoints().size());
        assertEquals(S1, plan.endpoints().get(0).getLocations().get(0).getUri().toString());
        verify(clusterService).addLoad(S1, 705L);

        Set<String> chosen = new HashSet<>();
        for (String tables : List.of("tpch.a|tpch.b", "tpch.c|tpch.d", "tpch.e|tpch.f",
                "tpch.g|tpch.h", "tpch.i|tpch.j", "tpch.k|tpch.l")) {
            chosen.add(QueryPlanner.leastLoadedServer(tables, Map.of(S1, 0L, S2, 0L)));
        }
        assertEquals(Set.of(S1, S2), chosen);
    }

    /** Verifies a table missing from the listing fails like a table without files. */
    @Test
    void withoutLocalityMissingTableFails() throws Exception {
        stubListing();
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1));
        when(clusterService.hasFileInventory(anyString())).thenReturn(true);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        assertThrows(IOException.class, () -> planner.plan("SELECT * FROM tpch.orders"));
    }

    private void stubListing() throws IOException {
        when(parquetAdapter.localFileInventory()).thenReturn(Map.of());
        lenient().when(parquetAdapter.withoutLocality()).thenReturn(true);
        lenient().when(parquetAdapter.getSchemas(null)).thenReturn(Map.of("tpch", "s3://lake/tpch"));
        lenient().when(parquetAdapter.getTables("tpch", null)).thenReturn(
                Map.of("lineitem", "s3://lake/tpch/lineitem", "nation", "s3://lake/tpch/nation"));
        lenient().when(parquetAdapter.tableFiles("tpch", "lineitem")).thenReturn(List.of(
                new pro.surpin.data.arrowflight.spi.storage.DataFile("tpch/lineitem/a.parquet",
                        "s3://lake/tpch/lineitem/a.parquet", 400L, 1L, Set.of()),
                new pro.surpin.data.arrowflight.spi.storage.DataFile("tpch/lineitem/b.parquet",
                        "s3://lake/tpch/lineitem/b.parquet", 300L, 1L, Set.of())));
        lenient().when(parquetAdapter.tableFiles("tpch", "nation")).thenReturn(List.of(
                new pro.surpin.data.arrowflight.spi.storage.DataFile("tpch/nation/n.parquet",
                        "s3://lake/tpch/nation/n.parquet", 5L, 1L, Set.of())));
    }
}
