package pro.surpin.data.arrowflight.plugin.hive;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Chooses what to read in one directory (table or partition) of a Hive insert-only (MM)
 * transactional table for a snapshot of valid write ids, following Hive's AcidUtils.getAcidState:
 * the newest usable base, the committed deltas above it, original files only without a base.
 * Layouts it cannot read correctly are refused, never read in part.
 */
final class InsertOnlyDirectories {

    private static final Pattern BASE = Pattern.compile("base_(\\d+)(?:_v(\\d+))?");
    private static final Pattern DELTA = Pattern.compile("delta_(\\d+)_(\\d+)(?:_(\\d+))?(?:_v(\\d+))?");
    private static final Pattern DELETE_DELTA = Pattern.compile("delete_delta_\\d+_\\d+.*");

    private InsertOnlyDirectories() {
    }

    /**
     * Write ids of one table as the metastore reports them for a snapshot.
     *
     * @param highWatermark highest allocated write id the snapshot sees
     * @param minOpenWriteId lowest write id still open, {@link Long#MAX_VALUE} without one
     * @param invalid open and aborted write ids up to the high watermark
     */
    record WriteIds(long highWatermark, long minOpenWriteId, Set<Long> invalid) {

        /**
         * Copies the invalid set.
         */
        WriteIds {
            invalid = Set.copyOf(invalid);
        }

        /**
         * Tells whether a write id is committed in the snapshot.
         *
         * @param writeId write id
         * @return true when committed
         */
        boolean valid(long writeId) {
            return writeId <= highWatermark && !invalid.contains(writeId);
        }

        /**
         * Tells whether a base of a write id is complete for the snapshot (nothing open below it).
         *
         * @param writeId the base's write id
         * @return true when the base may be read
         */
        boolean validBase(long writeId) {
            return writeId <= highWatermark && writeId < minOpenWriteId;
        }
    }

    /**
     * Transactions of a snapshot, for the visibility suffix ({@code _v<txn>}) of compacted directories.
     *
     * @param highWatermark highest transaction id the snapshot sees
     * @param invalid open and aborted transaction ids up to the high watermark
     */
    record Txns(long highWatermark, Set<Long> invalid) {

        /**
         * Copies the invalid set.
         */
        Txns {
            invalid = Set.copyOf(invalid);
        }

        /**
         * Tells whether a transaction is committed in the snapshot.
         *
         * @param txnId transaction id
         * @return true when committed
         */
        boolean committed(long txnId) {
            return txnId <= highWatermark && !invalid.contains(txnId);
        }
    }

    /**
     * One child of the directory.
     *
     * @param name file or directory name
     * @param directory whether it is a directory
     */
    record Entry(String name, boolean directory) {
    }

    /**
     * A parsed delta directory.
     *
     * @param name directory name
     * @param min lowest write id
     * @param max highest write id
     * @param statement statement id, -1 without one
     * @param visibility visibility transaction id, -1 without one
     */
    private record Delta(String name, long min, long max, long statement, long visibility) {
    }

    /**
     * Chooses the entries to read, in read order (base, deltas by write id, or originals).
     *
     * @param entries children of the table or partition directory
     * @param writeIds the table's write ids in the snapshot
     * @param txns the snapshot's transactions
     * @param compactedBase tells whether a base directory (by name) was written by the compactor
     *     (its {@code _metadata_acid} file)
     * @return names of the files and directories to read (directories recursively)
     * @throws IllegalStateException when the layout cannot be read correctly for the snapshot
     */
    static List<String> select(List<Entry> entries, WriteIds writeIds, Txns txns,
            Predicate<String> compactedBase) {
        String base = null;
        long baseId = -1;
        String unusableCompacted = null;
        List<Delta> deltas = new ArrayList<>();
        List<String> originals = new ArrayList<>();
        for (Entry entry : entries) {
            String name = entry.name();
            if (name.startsWith("_") || name.startsWith(".")) {
                continue;
            }
            Matcher b = BASE.matcher(name);
            Matcher d = DELTA.matcher(name);
            if (entry.directory() && b.matches()) {
                long id = Long.parseLong(b.group(1));
                boolean visible = b.group(2) != null;
                boolean compacted = visible || compactedBase.test(name);
                boolean usable = writeIds.validBase(id) && (visible
                        ? txns.committed(Long.parseLong(b.group(2)))
                        : compacted || writeIds.valid(id));
                if (usable && id > baseId) {
                    base = name;
                    baseId = id;
                } else if (compacted && !writeIds.validBase(id)) {
                    // (A compaction whose transaction is not committed yet is only left out.)
                    unusableCompacted = name;
                }
            } else if (entry.directory() && d.matches()) {
                deltas.add(new Delta(name, Long.parseLong(d.group(1)), Long.parseLong(d.group(2)),
                        d.group(3) == null ? -1 : Long.parseLong(d.group(3)),
                        d.group(4) == null ? -1 : Long.parseLong(d.group(4))));
            } else if (entry.directory() && DELETE_DELTA.matcher(name).matches()) {
                throw new IllegalStateException("delete delta " + name
                        + " in an insert-only table (full ACID layout)");
            } else {
                originals.add(name);
            }
        }
        if (base == null && unusableCompacted != null) {
            // The compactor's base replaced older directories the snapshot would need.
            throw new IllegalStateException("not enough history: compacted " + unusableCompacted
                    + " is newer than the snapshot (write id high watermark "
                    + writeIds.highWatermark() + ")");
        }
        List<String> result = new ArrayList<>();
        if (base != null) {
            result.add(base);
        } else {
            originals.sort(Comparator.naturalOrder());
            result.addAll(originals);
        }
        deltas.sort(Comparator.comparingLong(Delta::min)
                .thenComparing(Comparator.comparingLong(Delta::max).reversed())
                .thenComparingLong(Delta::statement));
        long covered = baseId;
        Delta last = null;
        for (Delta delta : deltas) {
            if (delta.max() <= baseId || !readable(delta, writeIds, txns)) {
                continue;
            }
            if (delta.max() > covered) {
                if (delta.min() <= covered) {
                    throw new IllegalStateException("delta " + delta.name()
                            + " overlaps the directories read before it");
                }
                result.add(delta.name());
                covered = delta.max();
                last = delta;
            } else if (last != null && last.min() == delta.min() && last.max() == delta.max()
                    && last.statement() != delta.statement()) {
                result.add(delta.name());   // another statement of the same write
            }
        }
        return List.copyOf(result);
    }

    /**
     * Tells whether a delta's rows are all committed in the snapshot.
     *
     * @param delta the delta
     * @param writeIds write ids
     * @param txns transactions
     * @return true to read it, false to leave it out (aborted, open, or a compaction not yet visible)
     * @throws IllegalStateException for a delta mixing committed and uncommitted write ids
     */
    private static boolean readable(Delta delta, WriteIds writeIds, Txns txns) {
        if (delta.visibility() >= 0 && !txns.committed(delta.visibility())) {
            return false;   // compaction not committed: its source deltas are still there
        }
        if (delta.min() == delta.max()) {
            return writeIds.valid(delta.min());
        }
        if (delta.max() > writeIds.highWatermark() || delta.max() >= writeIds.minOpenWriteId()) {
            throw new IllegalStateException("delta " + delta.name() + " reaches past the snapshot"
                    + " (write id high watermark " + writeIds.highWatermark() + ")");
        }
        if (delta.visibility() < 0) {
            for (long id : writeIds.invalid()) {
                if (id >= delta.min() && id <= delta.max()) {
                    // Without a visibility suffix nothing says the aborted rows were dropped.
                    throw new IllegalStateException("delta " + delta.name()
                            + " holds aborted write id " + id);
                }
            }
        }
        return true;
    }
}
