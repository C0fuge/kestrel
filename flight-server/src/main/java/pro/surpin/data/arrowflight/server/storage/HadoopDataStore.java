package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.adapters.HostUtils;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * Data directory on a Hadoop file system: HDFS, or a local directory through Hadoop's local file
 * system. Files carry the hosts of their blocks; on a local file system the blocks are on a
 * loopback address, which stands for the configured storage host.
 */
public final class HadoopDataStore implements DataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopDataStore.class);
    private static final String PARQUET_EXTENSION = ".parquet";

    private final FileSystem fileSystem;
    private final String dataDirectory;
    private final String storageHost;
    /** The data directory as the file system names it; looked up once, it never changes. */
    private volatile URI rootUri;

    /**
     * Creates a store over a data directory.
     *
     * @param fileSystem file system of the data directory
     * @param dataDirectory data directory as configured
     * @param storageHost storage host colocated with this server; loopback block hosts map to it
     */
    public HadoopDataStore(FileSystem fileSystem, String dataDirectory, String storageHost) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem");
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory");
        this.storageHost = storageHost;
    }

    @Override
    public String kind() {
        String scheme = fileSystem.getUri().getScheme();
        return scheme == null ? "file" : scheme;
    }

    @Override
    public boolean shared() {
        String scheme = fileSystem.getUri().getScheme();
        return scheme != null && !"file".equalsIgnoreCase(scheme);
    }

    @Override
    public boolean hasLocality() {
        return true;
    }

    @Override
    public String root() {
        return dataDirectory;
    }

    @Override
    public boolean exists(String relativePath) throws IOException {
        return fileSystem.exists(resolve(relativePath));
    }

    @Override
    public Map<String, String> directories(String relativePath) throws IOException {
        return Arrays.stream(fileSystem.listStatus(resolve(relativePath)))
                .filter(FileStatus::isDirectory)
                .collect(Collectors.toMap(status -> status.getPath().getName(),
                        status -> status.getPath().toString()));
    }

    @Override
    public List<DataFile> parquetFiles(String relativeDirectory) throws IOException {
        List<LocatedFileStatus> statuses = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator =
                fileSystem.listFiles(resolve(relativeDirectory), true);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            if (isParquetFile(file)) {
                statuses.add(file);
            }
        }
        statuses.sort(Comparator.comparing(file -> file.getPath().toString()));
        if (statuses.isEmpty()) {
            return List.of();
        }
        URI root = rootUri();
        List<DataFile> result = new ArrayList<>(statuses.size());
        for (LocatedFileStatus file : statuses) {
            result.add(dataFile(root, file, fileLocality(file).keySet()));
        }
        return result;
    }

    @Override
    public List<DataFile> files(String relativeDirectory, boolean recursive) throws IOException {
        List<LocatedFileStatus> statuses = new ArrayList<>();
        collectFiles(resolve(relativeDirectory), recursive, statuses);
        statuses.sort(Comparator.comparing(file -> file.getPath().toString()));
        URI root = rootUri();
        List<DataFile> result = new ArrayList<>(statuses.size());
        for (LocatedFileStatus file : statuses) {
            result.add(dataFile(root, file, fileLocality(file).keySet()));
        }
        return result;
    }

    /**
     * Collects the visible files of a directory, skipping names that start with "_" or ".".
     *
     * @param directory directory
     * @param recursive whether to descend into visible subdirectories
     * @param into receives the files
     * @throws IOException on storage access failure, including a missing directory
     */
    private void collectFiles(Path directory, boolean recursive, List<LocatedFileStatus> into)
            throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listLocatedStatus(directory);
        while (iterator.hasNext()) {
            LocatedFileStatus status = iterator.next();
            String name = status.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) {
                continue;
            }
            if (status.isFile()) {
                into.add(status);
            } else if (recursive && status.isDirectory()) {
                collectFiles(status.getPath(), true, into);
            }
        }
    }

    @Override
    public DataFile file(String relativePath) throws IOException {
        Path path = resolve(relativePath);
        FileStatus status = fileSystem.getFileStatus(path);
        return new DataFile(relativePath, path.toUri().toString(), status.getLen(),
                status.getModificationTime(), Set.of());
    }

    @Override
    public InputFile inputFile(String relativePath) throws IOException {
        Path path = resolve(relativePath);
        return inputFile(path, fileSystem.getFileStatus(path).getLen());
    }

    @Override
    public InputFile inputFile(DataFile file) {
        return inputFile(new Path(URI.create(file.location())), file.size());
    }

    @Override
    public ReadableFile open(String relativePath) throws IOException {
        Path path = resolve(relativePath);
        return readableFile(path, fileSystem.getFileStatus(path).getLen());
    }

    @Override
    public ReadableFile open(DataFile file) {
        return readableFile(new Path(URI.create(file.location())), file.size());
    }

    @Override
    public String duckDbUri(String relativePath) {
        return duckDbPath(fileSystem.makeQualified(resolve(relativePath)).toUri().toString());
    }

    @Override
    public String duckDbUri(DataFile file) {
        return duckDbPath(fileSystem.makeQualified(new Path(URI.create(file.location())))
                .toUri().toString());
    }

    @Override
    public List<String> duckDbRoots() {
        // The data directory as DuckDB sees file paths: qualified, local paths without "file:".
        String root = fileSystem.makeQualified(new Path(dataDirectory)).toUri().toString();
        if (root.startsWith("file:")) {
            root = URI.create(root).getPath();
        }
        return List.of(root);
    }

    @Override
    public Map<String, Long> localInventory(String host) throws IOException {
        long t = LogUtil.mark();
        Map<String, Long> result = new LinkedHashMap<>();
        Path root = new Path(this.dataDirectory);
        if (!this.fileSystem.exists(root)) {
            LOGGER.warn("node={} parquet=inventoryDataDirNotFound path={}",
                    LogUtil.node(), this.dataDirectory);
            return result;
        }

        URI base = rootUri();
        RemoteIterator<LocatedFileStatus> files = this.fileSystem.listFiles(root, true);
        int scanned = 0;
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            scanned++;
            if (isParquetFile(file) && hasLocalBlock(fileLocality(file).keySet(), host)) {
                // Decoded, as every other relative path: "part 1.parquet", not "part%201.parquet".
                String relativePath = base.relativize(file.getPath().toUri()).getPath();
                result.put(relativePath, file.getLen());
            }
        }
        long totalBytes = result.values().stream().mapToLong(Long::longValue).sum();
        LogUtil.logTiming(t, "parquet.localInventory", "localFiles=" + result.size()
                + " scanned=" + scanned + " totalBytes=" + totalBytes);
        return result;
    }

    /**
     * Tells whether one of a file's block hosts is the given storage host.
     *
     * @param blockHosts hosts of the file's blocks
     * @param localHost storage host colocated with this server
     * @return true when a block host resolves to the same host
     */
    static boolean hasLocalBlock(Set<String> blockHosts, String localHost) {
        String normalizedLocalHost = HostUtils.normalize(localHost);
        return blockHosts.stream()
                .map(HostUtils::normalize)
                .anyMatch(normalizedLocalHost::equals);
    }

    /**
     * Returns block locality for a file, mapping host to block count.
     *
     * @param file listed file
     * @return host to block count, fewest blocks first
     */
    LinkedHashMap<String, Long> fileLocality(LocatedFileStatus file) {
        BlockLocation[] blocks = file.getBlockLocations();
        if (blocks == null) {
            return new LinkedHashMap<>();
        }
        return Arrays.stream(blocks)
                .flatMap(bl -> {
                    try {
                        return Stream.of(bl.getHosts())
                                .map(host -> HostUtils.LOOPBACK_HOSTS.contains(host)
                                        ? storageHost : host);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.groupingBy(h -> h, Collectors.counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue()
                        .thenComparing(Map.Entry.comparingByKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
    }

    /**
     * Converts a qualified URI to the path DuckDB reads: local files become plain paths, with
     * escaped characters decoded ({@code file:/a/part%201.parquet} → {@code /a/part 1.parquet}).
     *
     * @param uri qualified URI
     * @return DuckDB path
     */
    static String duckDbPath(String uri) {
        if (uri.startsWith("file:")) {
            // file:///path and file:/path both name /path.
            return URI.create(uri).getPath();
        }
        return uri;
    }

    /**
     * Tells whether a listed entry is a Parquet file; the extension is matched in any case.
     *
     * @param file listed entry
     * @return true for a file whose name ends with ".parquet"
     */
    private static boolean isParquetFile(LocatedFileStatus file) {
        return file.isFile() && file.getPath().getName().toLowerCase().endsWith(PARQUET_EXTENSION);
    }

    /**
     * Describes a listed file.
     *
     * @param root data directory as the file system names it
     * @param file listed file
     * @param hosts hosts of the file's blocks
     * @return data file
     */
    private static DataFile dataFile(URI root, LocatedFileStatus file, Set<String> hosts) {
        URI uri = file.getPath().toUri();
        return new DataFile(root.relativize(uri).getPath(), uri.toString(), file.getLen(),
                file.getModificationTime(), hosts);
    }

    /**
     * Resolves a relative path below the data directory.
     *
     * @param relativePath path below the data directory, empty for the data directory
     * @return Hadoop path
     */
    private Path resolve(String relativePath) {
        return relativePath == null || relativePath.isEmpty()
                ? new Path(dataDirectory) : new Path(dataDirectory, relativePath);
    }

    /**
     * Returns the data directory as the file system names it, which listed paths are relative to.
     *
     * @return qualified data directory URI
     * @throws IOException when the data directory cannot be looked up
     */
    private URI rootUri() throws IOException {
        URI result = rootUri;
        if (result == null) {
            result = fileSystem.getFileStatus(new Path(dataDirectory)).getPath().toUri();
            rootUri = result;
        }
        return result;
    }

    /**
     * Opens a file whose length is known for positional reads; every read opens the file. The
     * server reads footers through {@link #inputFile(DataFile)}, which keeps one stream.
     *
     * @param path file path
     * @param length file length
     * @return readable file
     */
    private ReadableFile readableFile(Path path, long length) {
        return new ReadableFile() {
            @Override
            public long length() {
                return length;
            }

            @Override
            public void readFully(long position, byte[] buffer, int offset, int count)
                    throws IOException {
                try (FSDataInputStream input = fileSystem.open(path)) {
                    input.readFully(position, buffer, offset, count);
                }
            }

            @Override
            public String toString() {
                return path.toString();
            }
        };
    }

    /**
     * Opens a file whose length is known.
     *
     * @param path file path
     * @param length file length
     * @return Parquet input file
     */
    private InputFile inputFile(Path path, long length) {
        return new InputFile() {
            @Override
            public long getLength() {
                return length;
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                return HadoopStreams.wrap(fileSystem.open(path));
            }

            @Override
            public String toString() {
                return path.toString();
            }
        };
    }
}
