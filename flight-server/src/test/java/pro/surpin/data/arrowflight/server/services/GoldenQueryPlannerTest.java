package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightEndpoint;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.Golden;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Golden test (e) of the plugin plan: the endpoints the planner makes for fixed inventories and
 * loads, with locality (HDFS with replicas, local disks) and without it (S3). Each endpoint is
 * written with its node, files, bytes and load accounting, followed by the load added per node.
 */
@Tag("unit")
class GoldenQueryPlannerTest {

    private static final String S1 = "grpc://127.0.0.1:32010";
    private static final String S2 = "grpc://127.0.0.2:32010";
    private static final String S3 = "grpc://127.0.0.3:32010";

    private static final List<String> QUERIES = List.of(
            "SELECT l_orderkey, l_quantity FROM tpch.lineitem WHERE l_quantity > 10",
            "SELECT count(*) FROM lineitem",
            "SELECT o_orderstatus, count(*) FROM TPCH.ORDERS GROUP BY o_orderstatus",
            "SELECT o.o_orderkey, l.l_quantity FROM tpch.orders o JOIN tpch.lineitem l"
                    + " ON o.o_orderkey = l.l_orderkey",
            "SELECT n.n_name FROM tpch.nation n JOIN tpch.region r ON n.n_regionkey = r.r_regionkey",
            "SELECT * FROM tpch.supplier");

    /** Verifies the endpoints of every scenario against the golden file. */
    @Test
    void endpointsForFixedInventories() throws Exception {
        StringBuilder out = new StringBuilder();

        // HDFS: shared storage with locality, files replicated on two of three nodes.
        Map<String, FileAssignment> hdfs = new LinkedHashMap<>();
        hdfs.put("tpch/lineitem/part-0.parquet", file(900, S1, S2));
        hdfs.put("tpch/lineitem/part-1.parquet", file(800, S2, S3));
        hdfs.put("tpch/lineitem/part-2.parquet", file(700, S3, S1));
        hdfs.put("tpch/lineitem/part-3.parquet", file(600, S1, S2));
        hdfs.put("tpch/lineitem/part-4.parquet", file(500, S2, S3));
        hdfs.put("tpch/orders/part-0.parquet", file(400, S1, S2));
        hdfs.put("tpch/orders/part-1.parquet", file(300, S1, S3));
        hdfs.put("tpch/nation/part-0.parquet", file(10, S1));
        hdfs.put("tpch/region/part-0.parquet", file(5, S2, S3));
        hdfs.put("other/lineitem/part-0.parquet", file(50, S3));
        hdfs.put("tpch/supplier/part-0.parquet", file(70, S2));
        Map<String, Long> loads = new LinkedHashMap<>();
        loads.put(S1, 1000L);
        loads.put(S2, 0L);
        loads.put(S3, 500L);
        scenario(out, "hdfs-replicated loads=" + loads, true, true, hdfs, loads);

        Map<String, Long> idle = new LinkedHashMap<>();
        idle.put(S1, 0L);
        idle.put(S2, 0L);
        idle.put(S3, 0L);
        scenario(out, "hdfs-replicated idle", true, true, hdfs, idle);

        Map<String, Long> one = Map.of(S1, 0L);
        Map<String, FileAssignment> single = new LinkedHashMap<>();
        hdfs.forEach((path, f) -> single.put(path, file(f.size(), S1)));
        scenario(out, "hdfs-single-node", true, true, single, one);

        // Local disks: every node holds one shard of every table, nothing is shared.
        Map<String, FileAssignment> local = new LinkedHashMap<>();
        for (String table : List.of("lineitem", "orders", "nation", "region", "supplier")) {
            int i = 0;
            for (String server : List.of(S1, S2, S3)) {
                local.put("tpch/" + table + "/" + server.substring(7, 16) + "-part-" + i + ".parquet",
                        file(100L * (i + 1) + table.length(), server));
                i++;
            }
        }
        scenario(out, "local-disks idle", false, true, local, idle);

        // S3: no locality, files listed from the store.
        scenario(out, "s3 loads=" + loads, true, false, hdfs, loads);
        scenario(out, "s3 idle", true, false, hdfs, idle);
        scenario(out, "s3-single-node", true, false, hdfs, one);

        Golden.assertMatches("query-planner.txt", out.toString());
    }

    /**
     * Plans every query against one cluster and appends the endpoints.
     *
     * @param out text to append to
     * @param name scenario name
     * @param shared whether every node reads every file
     * @param locality whether files have hosts (HDFS, local disks) or not (S3)
     * @param files inventory: relative path to size and owning nodes
     * @param loads node loads before planning
     * @throws Exception when a mock cannot be set up
     */
    private static void scenario(StringBuilder out, String name, boolean shared, boolean locality,
            Map<String, FileAssignment> files, Map<String, Long> loads) throws Exception {
        out.append("# scenario ").append(name).append(" shared=").append(shared)
                .append(" locality=").append(locality).append('\n');
        for (String query : QUERIES) {
            ClusterService cluster = mock(ClusterService.class);
            ParquetAdapter parquet = mock(ParquetAdapter.class);
            List<HandleState> handles = new ArrayList<>();
            Map<String, Long> added = new TreeMap<>();
            when(cluster.allServerLoads()).thenReturn(new LinkedHashMap<>(loads));
            when(cluster.filterLiveServers(anySet())).thenAnswer(
                    inv -> new LinkedHashSet<>((Set<?>) inv.getArgument(0)));
            when(cluster.hasFileInventory(anyString())).thenReturn(true);
            when(cluster.fileLocations()).thenReturn(locality ? files : Map.of());
            when(cluster.createEndpointHandle(any())).thenAnswer(inv -> {
                handles.add(inv.getArgument(0));
                return ByteBuffer.allocate(4).putInt(handles.size() - 1).array();
            });
            doAnswer(inv -> {
                added.merge(inv.getArgument(0), inv.getArgument(1), Long::sum);
                return null;
            }).when(cluster).addLoad(anyString(), anyLong());
            when(parquet.localFileInventory()).thenReturn(Map.of());
            when(parquet.sharedStorage()).thenReturn(shared);
            when(parquet.withoutLocality()).thenReturn(!locality);
            when(parquet.estimateRowCount(anyMap())).thenAnswer(inv -> {
                Map<String, FileAssignment> assigned = inv.getArgument(0);
                return assigned.values().stream().mapToLong(f -> f.size() / 10).sum();
            });
            Map<String, Map<String, List<DataFile>>> listing = listing(files);
            Map<String, String> schemas = new TreeMap<>();
            listing.keySet().forEach(s -> schemas.put(s, "/" + s));
            when(parquet.getSchemas(isNull())).thenReturn(schemas);
            for (Map.Entry<String, Map<String, List<DataFile>>> schema : listing.entrySet()) {
                Map<String, String> tables = new TreeMap<>();
                schema.getValue().keySet().forEach(t -> tables.put(t, "/" + schema.getKey() + "/" + t));
                when(parquet.getTables(eq(schema.getKey()), isNull())).thenReturn(tables);
                for (Map.Entry<String, List<DataFile>> table : schema.getValue().entrySet()) {
                    when(parquet.tableFiles(schema.getKey(), table.getKey())).thenReturn(table.getValue());
                }
            }

            out.append("## ").append(query).append('\n');
            try {
                QueryPlan plan = new QueryPlanner(parquet, cluster).plan(query);
                out.append("totalBytes=").append(plan.totalBytes())
                        .append(" totalRecords=").append(plan.totalRecords()).append('\n');
                for (FlightEndpoint endpoint : plan.endpoints()) {
                    out.append("endpoint ").append(endpoint.getLocations().get(0).getUri());
                    HandleState state = handles.get(indexOf(endpoint, handles));
                    out.append(" bytes=").append(state.bytes())
                            .append(" loadTracked=").append(state.loadTracked())
                            .append(" server=").append(state.serverUri())
                            .append(" files=").append(List.of(state.filePaths())).append('\n');
                }
                out.append("addLoad ").append(added).append('\n');
            } catch (Exception e) {
                out.append("error ").append(e.getClass().getSimpleName()).append(": ")
                        .append(e.getMessage()).append('\n');
            }
        }
    }

    /**
     * Finds the handle state an endpoint's ticket refers to.
     *
     * @param endpoint endpoint
     * @param handles states in the order their handles were made
     * @return index of the state
     * @throws Exception when the ticket cannot be read
     */
    private static int indexOf(FlightEndpoint endpoint, List<HandleState> handles) throws Exception {
        com.google.protobuf.Any any = com.google.protobuf.Any.parseFrom(endpoint.getTicket().getBytes());
        byte[] handle = any.unpack(org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery.class)
                .getStatementHandle().toByteArray();
        return ByteBuffer.wrap(handle).getInt();
    }

    /**
     * Groups an inventory into schema, table and files, the store listing of S3.
     *
     * @param files inventory
     * @return schema to table to files
     */
    private static Map<String, Map<String, List<DataFile>>> listing(Map<String, FileAssignment> files) {
        Map<String, Map<String, List<DataFile>>> result = new TreeMap<>();
        for (Map.Entry<String, FileAssignment> file : new TreeMap<>(files).entrySet()) {
            String[] parts = file.getKey().split("/");
            result.computeIfAbsent(parts[0], k -> new TreeMap<>())
                    .computeIfAbsent(parts[1], k -> new ArrayList<>())
                    .add(new DataFile(file.getKey(), "s3://lake/parquet/" + file.getKey(),
                            file.getValue().size(), 0L, Set.of()));
        }
        return result;
    }

    /**
     * Builds an inventory entry.
     *
     * @param size file size
     * @param hosts nodes holding the file
     * @return file assignment
     */
    private static FileAssignment file(long size, String... hosts) {
        return new FileAssignment(size, new LinkedHashSet<>(List.of(hosts)));
    }
}
