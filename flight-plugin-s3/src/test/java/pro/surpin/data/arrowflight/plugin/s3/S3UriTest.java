package pro.surpin.data.arrowflight.plugin.s3;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests parsing S3 data directories and naming objects below them. */
@Tag("unit")
class S3UriTest {

    /** Verifies bucket and prefix, with s3a and s3n normalised to s3. */
    @Test
    void parsesBucketAndPrefix() {
        S3Uri uri = S3Uri.parse("s3a://lake/parquet/tpch/");

        assertEquals("lake", uri.bucket());
        assertEquals("parquet/tpch/", uri.prefix());
        assertEquals("s3://lake/parquet/tpch", uri.uri());
        assertEquals("s3://lake/parquet/tpch/", uri.root());
        assertEquals(new S3Uri("lake", "parquet/"), S3Uri.parse("S3N://lake/parquet"));
        assertEquals(new S3Uri("lake", "parquet/"), S3Uri.parse(" s3://lake/parquet// "));
    }

    /** Verifies a bucket without a prefix. */
    @Test
    void bucketOnly() {
        S3Uri uri = S3Uri.parse("s3://lake");

        assertEquals("", uri.prefix());
        assertEquals("s3://lake", uri.uri());
        assertEquals("s3://lake/", uri.root());
        assertEquals("tpch/a.parquet", uri.key("tpch/a.parquet"));
        assertEquals("s3://lake/tpch/a.parquet", uri.objectUri(uri.key("tpch/a.parquet")));
    }

    /** Verifies relative paths become keys below the prefix. */
    @Test
    void keysBelowThePrefix() {
        S3Uri uri = S3Uri.parse("s3://lake/parquet");

        assertEquals("parquet/", uri.key(""));
        assertEquals("parquet/", uri.key(null));
        assertEquals("parquet/tpch/lineitem", uri.key("tpch/lineitem/"));
        assertEquals("parquet/tpch/nation/part=1/n.parquet", uri.key("tpch/nation/part=1/n.parquet"));
    }

    /** Verifies paths that are absolute, climb out or carry URL syntax are refused. */
    @Test
    void unsafePathsAreRefused() {
        S3Uri uri = S3Uri.parse("s3://lake/parquet");

        for (String path : List.of("/etc/passwd", "../x.parquet", "tpch/../../x.parquet", "tpch/./x",
                "tpch//x.parquet", "x.parquet?s3_endpoint=evil:1", "x.parquet#frag", "tpch/*.parquet",
                "tpch/[ab].parquet", "tpch/{a,b}.parquet", "it's.parquet", "a\\b.parquet",
                "tpch/%2e%2e/x.parquet", "tpch%2F..%2Fx.parquet", "tab\tname")) {
            assertThrows(IllegalArgumentException.class, () -> uri.key(path), path);
        }
    }

    /** Verifies data directories that are not S3 buckets or unsafe are refused. */
    @Test
    void invalidDataDirectoriesAreRefused() {
        for (String uri : List.of("hdfs://ns/data", "/data/parquet", "s3://", "s3://a",
                "s3://user@host/x", "s3://host:9000/x", "s3://lake/../x", "s3://lake/a?b",
                "s3://-lake/x")) {
            assertThrows(IllegalArgumentException.class, () -> S3Uri.parse(uri), uri);
        }
    }

    /** Verifies which data directories are S3. */
    @Test
    void recognisesS3Schemes() {
        assertTrue(S3Uri.isS3("s3://lake"));
        assertTrue(S3Uri.isS3("S3A://lake/x"));
        assertTrue(S3Uri.isS3("s3n://lake/x"));
        assertFalse(S3Uri.isS3("hdfs://lake/x"));
        assertFalse(S3Uri.isS3("/data/s3://x"));
        assertFalse(S3Uri.isS3(null));
    }
}
