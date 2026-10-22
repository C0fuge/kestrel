package pro.surpin.data.arrowflight.plugin.lakefs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.plugin.s3.S3DataStore;
import pro.surpin.data.arrowflight.plugin.s3.S3Settings;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/** Branch pinning over a fake S3 gateway (bucket = repository, first segment = ref). */
class LakeFsDataStoreTest {

    private FakeS3 gateway;
    private final AtomicLong clock = new AtomicLong();
    private final AtomicInteger resolutions = new AtomicInteger();
    private volatile String head = "c1";

    @BeforeEach
    void start() throws IOException {
        gateway = FakeS3.start();
        put("c1/wh/tpch/nation/part-0.parquet", "one");
        put("c2/wh/tpch/nation/part-0.parquet", "two");
        put("c2/wh/tpch/nation/part-1.parquet", "two-b");
        put("v1/wh/tpch/nation/part-0.parquet", "tag");
    }

    @AfterEach
    void stop() {
        gateway.close();
    }

    private void put(String key, String text) {
        gateway.put("lake", key, text.getBytes(StandardCharsets.UTF_8));
    }

    private LakeFsDataStore store(String root, boolean pin) {
        RefResolver resolver = (repository, ref) -> {
            resolutions.incrementAndGet();
            return "main".equals(ref) ? Optional.of(head) : Optional.empty();
        };
        return store(root, pin, resolver);
    }

    private LakeFsDataStore store(String root, boolean pin, RefResolver resolver) {
        S3Settings settings = new S3Settings(gateway.endpoint(), "us-east-1", "path", null, true,
                "AKIALAKEFS", "secret", null, 30, 4, true);
        return new LakeFsDataStore(LakeFsUri.parse(root), settings,
                S3DataStore.createClient(settings), resolver, pin, 1000, clock::get, Thread::new,
                "test");
    }

    private static List<String> paths(List<DataFile> files) {
        List<String> result = new ArrayList<>();
        for (DataFile file : files) {
            result.add(file.relativePath());
        }
        return result;
    }

    @Test
    void branchIsPinnedToItsHeadCommitAndPathsCarryIt() throws IOException {
        try (LakeFsDataStore store = store("lakefs://lake/main/wh", true)) {
            List<DataFile> files = store.tableFiles("tpch", "nation");
            assertEquals(List.of("@c1/tpch/nation/part-0.parquet"), paths(files));
            assertEquals("s3://lake/c1/wh/tpch/nation/part-0.parquet", files.get(0).location());
            assertEquals("s3://lake/c1/wh/tpch/nation/part-0.parquet",
                    store.duckDbUri("@c1/tpch/nation/part-0.parquet"));

            // The branch moves: a ticket planned before still reads c1.
            head = "c2";
            clock.addAndGet(2_000_000_000L);
            assertEquals(List.of("@c2/tpch/nation/part-0.parquet", "@c2/tpch/nation/part-1.parquet"),
                    paths(store.parquetFiles("tpch/nation")));
            assertEquals("s3://lake/c1/wh/tpch/nation/part-0.parquet",
                    store.duckDbUri("@c1/tpch/nation/part-0.parquet"));
            ReadableFile file = store.open("@c1/tpch/nation/part-0.parquet");
            byte[] bytes = new byte[3];
            file.readFully(0, bytes, 0, 3);
            assertEquals("one", new String(bytes, StandardCharsets.UTF_8));
        }
    }

    @Test
    void listingsWithinTheTtlShareOneResolution() throws IOException {
        try (LakeFsDataStore store = store("lakefs://lake/main/wh", true)) {
            store.directories("");
            store.directories("tpch");
            store.tableFiles("tpch", "nation");
            assertEquals(1, resolutions.get());
            clock.addAndGet(1_000_000_000L);
            store.tableFiles("tpch", "nation");
            assertEquals(2, resolutions.get());
        }
    }

    @Test
    void commitsAndTagsAndUnpinnedBranchesReadAsTheyAre() throws IOException {
        try (LakeFsDataStore store = store("lakefs://lake/v1/wh", true)) {
            assertEquals(List.of("tpch/nation/part-0.parquet"),
                    paths(store.tableFiles("tpch", "nation")));
            assertEquals("s3://lake/v1/wh/tpch/nation/part-0.parquet",
                    store.duckDbUri("tpch/nation/part-0.parquet"));
        }
        resolutions.set(0);
        try (LakeFsDataStore store = store("lakefs://lake/c2/wh", false)) {
            assertEquals(2, store.tableFiles("tpch", "nation").size());
            assertEquals(0, resolutions.get());
        }
    }

    @Test
    void readScopeKeepsOneCommitBeyondTheTtlAndDataVersionFollowsTheBranch() throws IOException {
        try (LakeFsDataStore store = store("lakefs://lake/main/wh", true)) {
            assertEquals("c1", store.dataVersion());
            try (DataStore.ReadScope outer = store.readScope()) {
                assertEquals(List.of("@c1/tpch/nation/part-0.parquet"),
                        paths(store.tableFiles("tpch", "nation")));
                head = "c2";
                clock.addAndGet(5_000_000_000L);
                try (DataStore.ReadScope inner = store.readScope()) {
                    assertEquals(List.of("@c1/tpch/nation/part-0.parquet"),
                            paths(store.tableFiles("tpch", "nation")));
                }
                // Still inside the outer scope: same commit, also for the version.
                assertEquals("c1", store.dataVersion());
                assertEquals(1, store.parquetFiles("tpch/nation").size());
            }
            assertEquals("c2", store.dataVersion());
            assertEquals(2, store.tableFiles("tpch", "nation").size());
        }
    }

    @Test
    void scopesOfOtherThreadsResolveOnTheirOwn() throws Exception {
        try (LakeFsDataStore store = store("lakefs://lake/main/wh", true);
                DataStore.ReadScope scope = store.readScope()) {
            assertEquals("c1", store.dataVersion());
            head = "c2";
            clock.addAndGet(5_000_000_000L);
            AtomicReference<String> other = new AtomicReference<>();
            Thread thread = new Thread(() -> {
                try {
                    other.set(store.dataVersion());
                } catch (IOException e) {
                    other.set(e.toString());
                }
            });
            thread.start();
            thread.join();
            assertEquals("c2", other.get());
            assertEquals("c1", store.dataVersion());
        }
    }

    @Test
    void tagsAndCommitsAreConfirmedOnceAndUnknownRefsFailClearly() throws IOException {
        AtomicInteger confirmations = new AtomicInteger();
        RefResolver resolver = new RefResolver() {
            @Override
            public Optional<String> branchHead(String repository, String ref) {
                resolutions.incrementAndGet();
                return Optional.empty();
            }

            @Override
            public String refCommit(String repository, String ref) throws IOException {
                confirmations.incrementAndGet();
                if (!"v1".equals(ref)) {
                    throw new IOException("lakeFS repository '" + repository
                            + "' has no branch, tag or commit '" + ref + "'");
                }
                return "c1";
            }
        };
        try (LakeFsDataStore store = store("lakefs://lake/v1/wh", true, resolver)) {
            store.tableFiles("tpch", "nation");
            clock.addAndGet(5_000_000_000L);
            store.tableFiles("tpch", "nation");
            assertEquals("v1", store.dataVersion());
            assertEquals(1, resolutions.get());
            assertEquals(1, confirmations.get());
        }
        try (LakeFsDataStore store = store("lakefs://lake/mian/wh", true, resolver)) {
            IOException e = assertThrows(IOException.class,
                    () -> store.tableFiles("tpch", "nation"));
            assertTrue(e.getMessage().contains("no branch, tag or commit 'mian'"), e.getMessage());
        }
    }

    @Test
    void duckDbReadsTheRepositoryThroughTheGateway() {
        try (LakeFsDataStore store = store("lakefs://lake/main/wh", true)) {
            assertEquals(List.of("s3://lake/"), store.duckDbRoots());
            assertEquals("lakefs", store.kind());
            assertTrue(store.shared());
        }
    }
}
