package pro.surpin.data.arrowflight.plugin.s3;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Blocks of S3 objects in memory, least recently used dropped first, bounded in bytes. A block
 * is named by its object's version (ETag), so a rewritten object never mixes with its old
 * blocks; those age out. A block is held in pieces of {@link Block#PIECE_BYTES}, so no array of
 * it is a humongous object for G1 (an array of at least half a heap region takes whole regions
 * of its own: a 1 MiB array in 1 MiB regions took 2 MiB of heap, doubling the cache's real size).
 *
 * <p>With admission on reuse, a block offered after a miss is kept only when the same block was
 * offered before and is still among the last {@code ghostEntries} names offered and not kept (a
 * list of names only, no bytes). A scan of more data than the cache then passes through without
 * pushing out the blocks that small queries read again; such blocks are kept from their second
 * read on.
 */
final class S3BlockCache {

    /**
     * Names one block.
     *
     * @param bucket bucket
     * @param key object key
     * @param etag ETag of the object version
     * @param index block number, offset / block size
     */
    record BlockKey(String bucket, String key, String etag, long index) {
    }

    /**
     * The cache's use at one moment.
     *
     * @param usedBytes bytes of all cached blocks
     * @param blocks cached blocks
     * @param hits blocks found
     * @param misses blocks not found
     * @param evictions blocks dropped to make room
     * @param notAdmitted blocks offered and not kept (admission on reuse)
     */
    record Use(long usedBytes, int blocks, long hits, long misses, long evictions, long notAdmitted) {
    }

    /** The bytes of one block, in pieces below half of G1's smallest region. */
    static final class Block {

        /** Bytes per piece: below half of G1's smallest region (1 MiB), so never humongous. */
        static final int PIECE_BYTES = 256 << 10;

        private final byte[][] pieces;
        private final int length;

        private Block(byte[][] pieces, int length) {
            this.pieces = pieces;
            this.length = length;
        }

        /**
         * Reads a block from a stream.
         *
         * @param in stream positioned at the block's first byte
         * @param length bytes of the block
         * @return the block
         * @throws IOException when the stream fails or ends before the block does
         */
        static Block read(InputStream in, int length) throws IOException {
            byte[][] pieces = new byte[(length + PIECE_BYTES - 1) / PIECE_BYTES][];
            for (int p = 0; p < pieces.length; p++) {
                int size = Math.min(PIECE_BYTES, length - p * PIECE_BYTES);
                pieces[p] = new byte[size];
                if (in.readNBytes(pieces[p], 0, size) != size) {
                    throw new EOFException("stream ended inside a block");
                }
            }
            return new Block(pieces, length);
        }

        /**
         * Copies bytes into a block.
         *
         * @param bytes the block's bytes
         * @return the block
         */
        static Block of(byte[] bytes) {
            try {
                return read(new ByteArrayInputStream(bytes), bytes.length);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        /**
         * Returns the block's size.
         *
         * @return bytes
         */
        int length() {
            return length;
        }

        /**
         * Writes part of the block.
         *
         * @param out where to write
         * @param from first byte, inclusive
         * @param to last byte, exclusive
         * @throws IOException when the stream fails
         */
        void write(OutputStream out, int from, int to) throws IOException {
            int at = from;
            while (at < to) {
                byte[] piece = pieces[at / PIECE_BYTES];
                int offset = at % PIECE_BYTES;
                int n = Math.min(to - at, piece.length - offset);
                out.write(piece, offset, n);
                at += n;
            }
        }
    }

    private final long capacityBytes;
    private final int ghostEntries;
    private final LinkedHashMap<BlockKey, Block> blocks = new LinkedHashMap<>(256, 0.75f, true);
    private final LinkedHashMap<BlockKey, Boolean> ghosts = new LinkedHashMap<>(256, 0.75f, false);
    private long notAdmitted;
    private long usedBytes;
    private long hits;
    private long misses;
    private long evictions;

    /**
     * Creates an empty cache.
     *
     * @param capacityBytes most bytes held
     */
    S3BlockCache(long capacityBytes) {
        this(capacityBytes, 0);
    }

    /**
     * Creates an empty cache, with admission on reuse when {@code ghostEntries} is positive.
     *
     * @param capacityBytes most bytes held
     * @param ghostEntries names of offered, not kept blocks remembered; 0 = every offered block is kept
     */
    S3BlockCache(long capacityBytes, int ghostEntries) {
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("cache capacity must be positive: " + capacityBytes);
        }
        if (ghostEntries < 0) {
            throw new IllegalArgumentException("ghost entries must be >= 0: " + ghostEntries);
        }
        this.capacityBytes = capacityBytes;
        this.ghostEntries = ghostEntries;
    }

    /**
     * Tells whether blocks are admitted on reuse only.
     *
     * @return true when a block is kept from its second offer on
     */
    boolean admitOnReuse() {
        return ghostEntries > 0;
    }

    /**
     * Offers a block read after a miss: kept at once, or with admission on reuse only when it
     * was offered recently before.
     *
     * @param key block name
     * @param block the block
     * @return whether the block was kept
     */
    synchronized boolean offer(BlockKey key, Block block) {
        if (ghostEntries > 0 && ghosts.remove(key) == null) {
            ghosts.put(key, Boolean.TRUE);
            if (ghosts.size() > ghostEntries) {
                Iterator<BlockKey> eldest = ghosts.keySet().iterator();
                eldest.next();
                eldest.remove();
            }
            notAdmitted++;
            return false;
        }
        put(key, block);
        return true;
    }

    /**
     * Returns a block and marks it recently used.
     *
     * @param key block name
     * @return the block, or null when not cached
     */
    synchronized Block get(BlockKey key) {
        Block block = blocks.get(key);
        if (block == null) {
            misses++;
        } else {
            hits++;
        }
        return block;
    }

    /**
     * Returns a block and marks it recently used, without counting a hit or miss; for a second
     * look after a first {@link #get} missed.
     *
     * @param key block name
     * @return the block, or null when not cached
     */
    synchronized Block peek(BlockKey key) {
        return blocks.get(key);
    }

    /**
     * Adds a block, dropping the least recently used ones until the cache fits its capacity. A
     * block larger than the capacity is not kept.
     *
     * @param key block name
     * @param block the block
     */
    synchronized void put(BlockKey key, Block block) {
        if (block.length() > capacityBytes) {
            return;
        }
        Block old = blocks.put(key, block);
        usedBytes += block.length() - (old == null ? 0 : old.length());
        Iterator<Map.Entry<BlockKey, Block>> eldest = blocks.entrySet().iterator();
        while (usedBytes > capacityBytes && eldest.hasNext()) {
            Map.Entry<BlockKey, Block> e = eldest.next();
            usedBytes -= e.getValue().length();
            eldest.remove();
            evictions++;
        }
    }

    /**
     * Returns the most bytes the cache holds.
     *
     * @return capacity in bytes
     */
    long capacityBytes() {
        return capacityBytes;
    }

    /**
     * Returns the bytes held.
     *
     * @return bytes of all cached blocks
     */
    synchronized long usedBytes() {
        return usedBytes;
    }

    /**
     * Returns the cache's use so far.
     *
     * @return counts taken together
     */
    synchronized Use use() {
        return new Use(usedBytes, blocks.size(), hits, misses, evictions, notAdmitted);
    }

    /**
     * Describes the cache's use so far.
     *
     * @return {@code usedBytes=... blocks=... hits=... misses=... evictions=... notAdmitted=...}
     */
    String stats() {
        Use use = use();
        return "usedBytes=" + use.usedBytes() + " blocks=" + use.blocks() + " hits=" + use.hits()
                + " misses=" + use.misses() + " evictions=" + use.evictions() + " notAdmitted=" + use.notAdmitted();
    }
}
