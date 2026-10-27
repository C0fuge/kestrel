package pro.surpin.data.arrowflight.plugin.s3;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Blocks of S3 objects in memory, least recently used dropped first, bounded in bytes. A block
 * is named by its object's version (ETag), so a rewritten object never mixes with its old
 * blocks; those age out.
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
     */
    record Use(long usedBytes, int blocks, long hits, long misses, long evictions) {
    }

    private final long capacityBytes;
    private final LinkedHashMap<BlockKey, byte[]> blocks = new LinkedHashMap<>(256, 0.75f, true);
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
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("cache capacity must be positive: " + capacityBytes);
        }
        this.capacityBytes = capacityBytes;
    }

    /**
     * Returns a block and marks it recently used.
     *
     * @param key block name
     * @return its bytes, or null when not cached
     */
    synchronized byte[] get(BlockKey key) {
        byte[] block = blocks.get(key);
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
     * @return its bytes, or null when not cached
     */
    synchronized byte[] peek(BlockKey key) {
        return blocks.get(key);
    }

    /**
     * Adds a block, dropping the least recently used ones until the cache fits its capacity. A
     * block larger than the capacity is not kept.
     *
     * @param key block name
     * @param block bytes; the cache keeps the array, the caller must not change it
     */
    synchronized void put(BlockKey key, byte[] block) {
        if (block.length > capacityBytes) {
            return;
        }
        byte[] old = blocks.put(key, block);
        usedBytes += block.length - (old == null ? 0 : old.length);
        Iterator<Map.Entry<BlockKey, byte[]>> eldest = blocks.entrySet().iterator();
        while (usedBytes > capacityBytes && eldest.hasNext()) {
            Map.Entry<BlockKey, byte[]> e = eldest.next();
            usedBytes -= e.getValue().length;
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
        return new Use(usedBytes, blocks.size(), hits, misses, evictions);
    }

    /**
     * Describes the cache's use so far.
     *
     * @return {@code usedBytes=... blocks=... hits=... misses=... evictions=...}
     */
    String stats() {
        Use use = use();
        return "usedBytes=" + use.usedBytes() + " blocks=" + use.blocks() + " hits=" + use.hits()
                + " misses=" + use.misses() + " evictions=" + use.evictions();
    }
}
