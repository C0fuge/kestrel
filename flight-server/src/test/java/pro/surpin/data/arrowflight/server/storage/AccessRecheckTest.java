package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Item 44 (2): a revoke on the storage must reach a table's repeat queries within the recheck
 * interval, although the server's caches would answer them without a storage call.
 */
class AccessRecheckTest {

    @TempDir
    java.nio.file.Path dataDir;

    /** One storage call per table per interval; a denial is seen once the interval has passed. */
    @Test
    void asksOncePerTablePerIntervalAndSeesARevokeAfterIt() throws IOException {
        AtomicLong now = new AtomicLong(1_000);
        List<String> asked = new ArrayList<>();
        boolean[] allowed = {true};
        AccessRecheck recheck = new AccessRecheck(10_000, directory -> {
            asked.add(directory);
            if (!allowed[0]) {
                throw new IOException("Permission denied: user=flight, access=READ_EXECUTE");
            }
            return true;
        }, now::get);

        assertTrue(recheck.require("tpch/orders"));
        assertFalse(recheck.require("tpch/orders"));
        assertTrue(recheck.require("tpch/nation"));
        assertEquals(List.of("tpch/orders", "tpch/nation"), asked);

        allowed[0] = false;   // the revoke
        now.addAndGet(TimeUnit.SECONDS.toNanos(9));
        assertFalse(recheck.require("tpch/orders"));   // still within the interval
        now.addAndGet(TimeUnit.SECONDS.toNanos(1));
        assertThrows(IOException.class, () -> recheck.require("tpch/orders"));
        // A denied table is asked again by every query, not trusted for an interval.
        assertThrows(IOException.class, () -> recheck.require("tpch/orders"));
        assertEquals(4, asked.size());

        allowed[0] = true;    // granted again
        assertTrue(recheck.require("tpch/orders"));
        assertFalse(recheck.require("tpch/orders"));
    }

    /** A storage that could not be asked is not trusted: the next query asks again. */
    @Test
    void anUnansweredCheckIsNotRemembered() throws IOException {
        int[] calls = {0};
        AccessRecheck recheck = new AccessRecheck(10_000, directory -> {
            calls[0]++;
            return false;
        }, () -> 0L);
        assertTrue(recheck.require("tpch/orders"));
        assertTrue(recheck.require("tpch/orders"));
        assertEquals(2, calls[0]);
        assertThrows(IllegalArgumentException.class, () -> new AccessRecheck(0, directory -> true));
    }

    /**
     * The Hadoop store's check goes through the file system's permission check: a directory the
     * server user may not read is a denial, a missing one is no answer.
     */
    @Test
    void hadoopStoreChecksTheDirectoryPermission() throws IOException {
        FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        Files.createDirectories(dataDir.resolve("tpch/orders"));
        HadoopDataStore store = new HadoopDataStore(fileSystem, dataDir.toString(), "localhost");
        assertTrue(store.checksAccess());
        assertTrue(store.checkReadable("tpch/orders"));
        assertFalse(store.checkReadable("tpch/missing"));
        Path orders = new Path(dataDir.resolve("tpch/orders").toString());
        fileSystem.setPermission(orders, new FsPermission((short) 0));
        try {
            IOException denied = assertThrows(IOException.class, () -> store.checkReadable("tpch/orders"));
            assertTrue(denied.getMessage().contains("Permission denied"), denied.getMessage());
        } finally {
            fileSystem.setPermission(orders, new FsPermission((short) 0755));
        }
    }

    /**
     * The adapter checks tables of the data directory by their directory, maps a denial to an
     * {@link UncheckedIOException} the producer reports as UNAUTHORIZED, and skips names it
     * cannot check.
     */
    @Test
    void adapterRechecksTablesOfTheDataDirectory() throws IOException {
        Files.createDirectories(dataDir.resolve("tpch/orders"));
        FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        AppConfig config = new AppConfig(
                3, 4096, 4, 65536, 2, 2, 2, null, false,
                null, null, null, null, null, false, 2_147_483_648L, 67108864, 67108864, 30000L,
                dataDir.toString(), null, 31001, 5701, 120, 3, 500, 0, true, false, 64, null, null,
                "complete", true, true);
        ParquetAdapter adapter = new ParquetAdapter(config,
                new HadoopDataStore(fileSystem, config.dataDir(), "localhost"));
        // A local data dir: off by default (the default 10 s applies to hdfs:// only).
        assertFalse(adapter.rechecksAccess());

        List<String> asked = new ArrayList<>();
        adapter.setAccessRecheck(new AccessRecheck(10_000, directory -> {
            asked.add(directory);
            if (directory.equals("tpch/secret")) {
                throw new org.apache.hadoop.security.AccessControlException(
                        "Permission denied: user=flight, access=READ_EXECUTE, inode=\"/data/tpch/secret\"");
            }
            return true;
        }, () -> 0L));
        assertTrue(adapter.rechecksAccess());
        adapter.recheckAccess("tpch", "orders");
        adapter.recheckAccess("tpch", "orders");
        adapter.recheckAccess(null, "orders");
        adapter.recheckAccess("tpch", "bad/name");
        UncheckedIOException denied = assertThrows(UncheckedIOException.class,
                () -> adapter.recheckAccess("tpch", "secret"));
        assertTrue(denied.getCause().getMessage().contains("Permission denied"));
        assertEquals(List.of("tpch/orders", "tpch/secret"), asked);
    }
}
