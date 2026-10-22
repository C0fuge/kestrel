package pro.surpin.data.arrowflight.spi.storage;

import java.util.Objects;

/**
 * An absolute location resolved to the store whose root holds it.
 *
 * @param store store with the longest root that holds the location
 * @param relativePath path below that store's root
 */
public record Located(DataStore store, String relativePath) {

    /**
     * Checks the parts.
     *
     * @param store store
     * @param relativePath relative path
     */
    public Located {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(relativePath, "relativePath");
    }
}
