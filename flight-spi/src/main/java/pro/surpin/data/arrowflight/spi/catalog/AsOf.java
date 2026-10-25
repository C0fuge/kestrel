package pro.surpin.data.arrowflight.spi.catalog;

import java.time.Instant;
import java.util.Objects;

/**
 * A point in a table's history that a query reads instead of the current snapshot (time travel,
 * {@code FOR VERSION AS OF} / {@code FOR TIMESTAMP AS OF}).
 *
 * @since SPI 1.2
 */
public sealed interface AsOf {

    /**
     * A snapshot named by its ID ({@code FOR VERSION AS OF 8213491011553364004}).
     *
     * @param id snapshot ID
     */
    record SnapshotId(long id) implements AsOf {
    }

    /**
     * The snapshot a named reference points at ({@code FOR VERSION AS OF 'tag'}): an Iceberg
     * branch or tag.
     *
     * @param name reference name
     */
    record Ref(String name) implements AsOf {

        /**
         * Checks the name.
         *
         * @param name reference name
         */
        public Ref {
            Objects.requireNonNull(name, "name");
        }
    }

    /**
     * The latest snapshot committed at or before an instant ({@code FOR TIMESTAMP AS OF}).
     *
     * @param at instant
     */
    record Timestamp(Instant at) implements AsOf {

        /**
         * Checks the instant.
         *
         * @param at instant
         */
        public Timestamp {
            Objects.requireNonNull(at, "at");
        }
    }
}
