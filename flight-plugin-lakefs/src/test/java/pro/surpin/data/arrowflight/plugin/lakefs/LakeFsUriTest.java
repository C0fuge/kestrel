package pro.surpin.data.arrowflight.plugin.lakefs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LakeFsUriTest {

    @Test
    void parsesRepositoryRefAndPrefix() {
        LakeFsUri uri = LakeFsUri.parse("lakefs://lake-repo/main/warehouse/parquet/");
        assertEquals("lake-repo", uri.repository());
        assertEquals("main", uri.ref());
        assertEquals("warehouse/parquet", uri.prefix());
        assertEquals("s3://lake-repo/main/warehouse/parquet", uri.s3Uri());
        assertEquals("s3://lake-repo/abc123/warehouse/parquet", uri.at("abc123").s3Uri());
        assertEquals("lakefs://lake-repo/main", LakeFsUri.parse("LAKEFS://lake-repo/main").uri());
    }

    @Test
    void refusesIncompleteOrUnsafeUris() {
        assertThrows(IllegalArgumentException.class, () -> LakeFsUri.parse("s3://repo/main"));
        assertThrows(IllegalArgumentException.class, () -> LakeFsUri.parse("lakefs://repo"));
        assertThrows(IllegalArgumentException.class, () -> LakeFsUri.parse("lakefs://Repo_X/main"));
        assertThrows(IllegalArgumentException.class, () -> LakeFsUri.parse("lakefs://repo/main~1/x"));
        assertThrows(IllegalArgumentException.class, () -> LakeFsUri.parse("lakefs://repo/../x"));
        assertThrows(IllegalArgumentException.class, () -> LakeFsUri.parse("lakefs://repo/main//x"));
        // Dot segments would leave the ref on the S3 gateway: refused before anything is asked.
        assertThrows(IllegalArgumentException.class,
                () -> LakeFsUri.parse("lakefs://repo/main/../tag/wh"));
        assertThrows(IllegalArgumentException.class,
                () -> LakeFsUri.parse("lakefs://repo/main/wh/./x"));
        assertThrows(IllegalArgumentException.class, () -> LakeFsUri.parse("lakefs://repo/main/.."));
    }
}
