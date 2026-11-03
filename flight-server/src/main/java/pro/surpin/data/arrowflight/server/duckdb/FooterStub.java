package pro.surpin.data.arrowflight.server.duckdb;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * A local Parquet file with the footer of a stored file and no row groups, so that DuckDB can
 * describe the stored file's columns from a footer read through the Java storage client. On HDFS
 * clusters whose DataNodes require SASL data-transfer protection, DuckDB's hadoopfs extension
 * cannot read a block without a local replica; the Java Hadoop client can.
 */
public final class FooterStub implements AutoCloseable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ENCRYPTED_MAGIC = "PARE".getBytes(StandardCharsets.US_ASCII);
    /** Largest footer copied; a longer length field means a damaged file. */
    private static final int MAX_FOOTER_BYTES = 256 << 20;

    private final Path directory;
    private final Path file;

    /**
     * Holds a written stub.
     *
     * @param directory directory of this stub only, removed on close
     * @param file the stub file
     */
    private FooterStub(Path directory, Path file) {
        this.directory = directory;
        this.file = file;
    }

    /**
     * Writes the stub of one stored file below a root. The stub keeps the stored file's path
     * below a directory of its own, so that DuckDB finds the same hive partition columns
     * ({@code key=value} directories) in it.
     *
     * @param source stored file, read through the storage client
     * @param root directory DuckDB may read (below its temp root)
     * @param location the stored file's DuckDB location ({@code hdfs://ns/dir/file.parquet})
     * @return the written stub
     * @throws IOException when the footer cannot be read or the stub cannot be written
     */
    public static FooterStub write(InputFile source, Path root, String location)
            throws IOException {
        byte[] bytes = stubBytes(source, location);
        Path directory = root.resolve(UUID.randomUUID().toString());
        Path file = directory.resolve(pathBelowAuthority(location)).normalize();
        if (!file.startsWith(directory) || file.equals(directory)) {
            throw new IOException("Parquet file location cannot be mapped to a local path: "
                    + location);
        }
        Files.createDirectories(file.getParent());
        FooterStub stub = new FooterStub(directory, file);
        try {
            Files.write(file, bytes);
        } catch (IOException e) {
            stub.close();
            throw e;
        }
        return stub;
    }

    /**
     * Returns the stub file.
     *
     * @return local path DuckDB reads
     */
    public Path file() {
        return file;
    }

    /** Removes the stub and its directory. */
    @Override
    public void close() {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            // Left below the temp root, which is removed when the server stops.
            org.slf4j.LoggerFactory.getLogger(FooterStub.class).debug(
                    "footerStub=notRemoved dir={} error='{}'", directory, e.getMessage());
        }
    }

    /**
     * Builds the stub's bytes: the stored file's footer without row groups.
     *
     * @param source stored file
     * @param location location named in errors
     * @return Parquet file bytes
     * @throws IOException when the file is not a readable Parquet file
     */
    static byte[] stubBytes(InputFile source, String location) throws IOException {
        long length = source.getLength();
        if (length < 2L * MAGIC.length + 4) {
            throw new IOException("Not a Parquet file (" + length + " bytes): " + location);
        }
        byte[] footer;
        try (SeekableInputStream in = source.newStream()) {
            byte[] tail = new byte[8];
            in.seek(length - tail.length);
            in.readFully(tail);
            byte[] magic = Arrays.copyOfRange(tail, 4, 8);
            if (Arrays.equals(magic, ENCRYPTED_MAGIC)) {
                throw new IOException("Parquet file has an encrypted footer: " + location);
            }
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IOException("Not a Parquet file (no PAR1 at the end): " + location);
            }
            int footerLength = ByteBuffer.wrap(tail, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if (footerLength <= 0 || footerLength > MAX_FOOTER_BYTES
                    || footerLength > length - tail.length - MAGIC.length) {
                throw new IOException("Parquet footer length " + footerLength
                        + " does not fit the file (" + length + " bytes): " + location);
            }
            footer = new byte[footerLength];
            in.seek(length - tail.length - footerLength);
            in.readFully(footer);
        }
        FileMetaData metadata = Util.readFileMetaData(new ByteArrayInputStream(footer));
        metadata.setRow_groups(new ArrayList<>());
        metadata.setNum_rows(0L);
        ByteArrayOutputStream serialized = new ByteArrayOutputStream(footer.length);
        Util.writeFileMetaData(metadata, serialized);
        ByteArrayOutputStream out = new ByteArrayOutputStream(serialized.size() + 12);
        out.write(MAGIC);
        serialized.writeTo(out);
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(serialized.size()).array());
        out.write(MAGIC);
        return out.toByteArray();
    }

    /**
     * Returns a location's path without scheme and authority, without a leading slash.
     *
     * @param location URI-like location or plain path
     * @return relative path
     */
    static String pathBelowAuthority(String location) {
        String path = location;
        int scheme = path.indexOf("://");
        if (scheme >= 0) {
            int slash = path.indexOf('/', scheme + 3);
            path = slash < 0 ? "" : path.substring(slash);
        }
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
}
