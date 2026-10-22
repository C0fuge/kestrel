package pro.surpin.data.arrowflight.spi.storage;

/**
 * What a store is opened for.
 */
public enum StoreUse {
    /** The data directory: schemas and tables below one root; the whole tree may be listed. */
    DATA_DIRECTORY,
    /** A catalog's warehouse root: only what a catalog names is looked up or listed. */
    CATALOG_ROOT
}
