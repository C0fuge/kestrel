package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.plugin.hive.InsertOnlyDirectories.Entry;
import pro.surpin.data.arrowflight.plugin.hive.InsertOnlyDirectories.Txns;
import pro.surpin.data.arrowflight.plugin.hive.InsertOnlyDirectories.WriteIds;

/**
 * Directory choice for insert-only tables: committed writes only, the newest usable base, and a
 * clear refusal for layouts that could give wrong rows.
 */
@Tag("unit")
class InsertOnlyDirectoriesTest {

    private static final Txns ALL_TXNS = new Txns(1000, Set.of());
    private static final Predicate<String> NOT_COMPACTED = name -> false;

    @Test
    void readsCommittedDeltasAndSkipsAbortedOpenAndNewerOnes() {
        WriteIds ids = new WriteIds(5, 4, Set.of(2L, 4L));
        assertEquals(List.of("delta_0000001_0000001_0000", "delta_0000003_0000003_0000",
                        "delta_0000005_0000005_0000"),
                select(ids, NOT_COMPACTED, "delta_0000001_0000001_0000", "delta_0000002_0000002_0000",
                        "delta_0000003_0000003_0000", "delta_0000004_0000004_0000",
                        "delta_0000005_0000005_0000", "delta_0000006_0000006_0000"));
    }

    @Test
    void originalFilesAreReadWithoutABaseAndHiddenEntriesNever() {
        WriteIds ids = new WriteIds(1, Long.MAX_VALUE, Set.of());
        assertEquals(List.of("000000_0", "HIVE_UNION_SUBDIR_1", "delta_0000001_0000001_0000"),
                select(ids, NOT_COMPACTED, "_tmp.delta_0000002_0000002_0000", ".hive-staging_1",
                        "delta_0000001_0000001_0000", "HIVE_UNION_SUBDIR_1/", "000000_0", "_SUCCESS"));
    }

    @Test
    void newestUsableBaseReplacesOriginalsAndOlderDeltas() {
        WriteIds ids = new WriteIds(4, Long.MAX_VALUE, Set.of());
        assertEquals(List.of("base_0000003", "delta_0000004_0000004_0000"),
                select(ids, NOT_COMPACTED, "000000_0", "base_0000002", "base_0000003",
                        "delta_0000001_0000001_0000", "delta_0000003_0000003_0000",
                        "delta_0000004_0000004_0000"));
    }

    @Test
    void abortedOrOpenInsertOverwriteBaseIsLeftOut() {
        WriteIds aborted = new WriteIds(3, Long.MAX_VALUE, Set.of(2L));
        assertEquals(List.of("delta_0000001_0000001_0000", "delta_0000003_0000003_0000"),
                select(aborted, NOT_COMPACTED, "delta_0000001_0000001_0000", "base_0000002",
                        "delta_0000003_0000003_0000"));
        WriteIds open = new WriteIds(2, 2, Set.of(2L));
        assertEquals(List.of("delta_0000001_0000001_0000"),
                select(open, NOT_COMPACTED, "delta_0000001_0000001_0000", "base_0000002"));
    }

    @Test
    void compactedBaseNeedsItsVisibilityTransactionOrMarker() {
        WriteIds ids = new WriteIds(3, Long.MAX_VALUE, Set.of(2L));
        // base_3_v20 by the compactor (it dropped aborted write id 2): read once txn 20 is committed.
        assertEquals(List.of("base_0000003_v0000020"),
                select(ids, NOT_COMPACTED, "delta_0000001_0000001_0000", "delta_0000003_0000003_0000",
                        "base_0000003_v0000020"));
        Txns compactionOpen = new Txns(1000, Set.of(20L));
        assertEquals(List.of("delta_0000001_0000001_0000", "delta_0000003_0000003_0000"),
                InsertOnlyDirectories.select(entries("delta_0000001_0000001_0000",
                        "delta_0000003_0000003_0000", "base_0000003_v0000020"), ids, compactionOpen,
                        NOT_COMPACTED));
        // A pre-visibility compacted base whose write id was aborted counts through its marker.
        WriteIds abortedTop = new WriteIds(3, Long.MAX_VALUE, Set.of(3L));
        assertEquals(List.of("base_0000003"), select(abortedTop, "base_0000003"::equals,
                "delta_0000001_0000001_0000", "base_0000003"));
    }

    @Test
    void compactedBaseNewerThanTheSnapshotIsRefused() {
        WriteIds ids = new WriteIds(2, Long.MAX_VALUE, Set.of());
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> select(ids, NOT_COMPACTED, "base_0000005_v0000030", "delta_0000006_0000006_0000"));
        assertTrue(e.getMessage().contains("not enough history"), e.getMessage());
    }

    @Test
    void statementsOfOneWriteAreAllReadAndCompactedDeltasCoverTheirSources() {
        WriteIds ids = new WriteIds(4, Long.MAX_VALUE, Set.of(2L));
        assertEquals(List.of("delta_0000001_0000003_v0000040", "delta_0000004_0000004_0000",
                        "delta_0000004_0000004_0001"),
                select(ids, NOT_COMPACTED, "delta_0000001_0000001_0000", "delta_0000002_0000002_0000",
                        "delta_0000003_0000003_0000", "delta_0000001_0000003_v0000040",
                        "delta_0000004_0000004_0001", "delta_0000004_0000004_0000"));
    }

    @Test
    void ambiguousLayoutsAreRefused() {
        WriteIds ids = new WriteIds(6, Long.MAX_VALUE, Set.of(2L));
        assertTrue(assertThrows(IllegalStateException.class, () -> select(ids, NOT_COMPACTED,
                "delta_0000001_0000003")).getMessage().contains("aborted write id 2"));
        assertTrue(assertThrows(IllegalStateException.class, () -> select(ids, NOT_COMPACTED,
                "delete_delta_0000003_0000003_0000")).getMessage().contains("full ACID"));
        assertTrue(assertThrows(IllegalStateException.class, () -> select(ids, NOT_COMPACTED,
                "delta_0000003_0000005", "delta_0000004_0000006")).getMessage().contains("overlaps"));
        assertTrue(assertThrows(IllegalStateException.class, () -> select(new WriteIds(4, 4, Set.of(4L)),
                NOT_COMPACTED, "delta_0000003_0000005_v0000009")).getMessage().contains("past the snapshot"));
    }

    @Test
    void emptyDirectoryReadsNothing() {
        assertEquals(List.of(), select(new WriteIds(0, Long.MAX_VALUE, Set.of()), NOT_COMPACTED));
    }

    private static List<String> select(WriteIds ids, Predicate<String> compacted, String... names) {
        return InsertOnlyDirectories.select(entries(names), ids, ALL_TXNS, compacted);
    }

    /**
     * Builds entries: names with acid prefixes or a trailing slash are directories.
     */
    private static List<Entry> entries(String... names) {
        List<Entry> result = new ArrayList<>();
        for (String name : names) {
            boolean dir = name.endsWith("/") || name.startsWith("base_") || name.startsWith("delta_")
                    || name.startsWith("delete_delta_") || name.startsWith("_tmp") || name.startsWith(".hive");
            result.add(new Entry(name.endsWith("/") ? name.substring(0, name.length() - 1) : name, dir));
        }
        return result;
    }
}
