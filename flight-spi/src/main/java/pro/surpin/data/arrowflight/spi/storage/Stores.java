package pro.surpin.data.arrowflight.spi.storage;

import java.io.IOException;

/**
 * The stores the server has open, for catalogs that name files by absolute URI.
 */
public interface Stores {

    /**
     * Finds the store whose root holds an absolute location.
     *
     * @param absoluteUri absolute URI or path
     * @return the store with the longest matching root and the path below it
     * @throws IOException when no open store holds the location
     */
    Located locate(String absoluteUri) throws IOException;
}
