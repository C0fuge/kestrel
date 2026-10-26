package pro.surpin.data.arrowflight.plugin.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.AttributeMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * Data directory below a key prefix of an S3 bucket, read only.
 *
 * <p>The whole prefix is listed flat (ListObjectsV2, 1000 keys a page) into a snapshot; schemas,
 * tables and their files come from the keys, sizes and modification times from the listing, so
 * no object is looked up one by one. A snapshot is used for {@code s3ListingTtlSeconds}; after
 * that the next use starts a new listing in the background and keeps getting the old snapshot
 * until the new one is complete. Objects have no hosts: every node reads every file equally.
 *
 * <p>The Java client reads only listings and Parquet footers; DuckDB reads the data through its
 * httpfs extension, set up by {@link #contributeDuckDb(DuckDbSetup)}.
 */
public final class S3DataStore implements DataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3DataStore.class);
    /** The server's timing log ("Timing", DEBUG), written in its format. */
    private static final Logger TIMING_LOG = LoggerFactory.getLogger("Timing");
    private static final String PARQUET_EXTENSION = ".parquet";
    /** Name of the DuckDB secret that holds the credentials. */
    static final String DUCKDB_SECRET_NAME = "flight_s3";
    /** Keys per listing page; S3 never returns more. */
    private static final int LIST_PAGE_SIZE = 1000;

    private final S3Uri root;
    private final S3Settings settings;
    private final S3Client client;
    private final long listingTtlNanos;
    private final LongSupplier nanoClock;
    private final String node;
    private final ExecutorService refresher;
    private final AtomicBoolean refreshing = new AtomicBoolean();
    private final Object initialListing = new Object();
    private volatile Snapshot snapshot;
    private final boolean ownsClient;
    private final String configName;

    /**
     * One complete listing of the data directory.
     *
     * @param loadedAtNanos monotonic time the listing started
     * @param files relative path to file, every object except directory markers
     * @param directories relative directories, without a trailing "/"
     */
    private record Snapshot(long loadedAtNanos, NavigableMap<String, DataFile> files,
            NavigableSet<String> directories) {
    }

    /**
     * Creates a store over a bucket prefix.
     *
     * @param dataDirectory data directory, {@code s3://bucket/prefix}
     * @param settings S3 settings
     * @param client S3 client; the store closes it
     */
    public S3DataStore(String dataDirectory, S3Settings settings, S3Client client) {
        this(dataDirectory, settings, client, System::nanoTime);
    }

    /**
     * Creates a store with its own clock, for tests.
     *
     * @param dataDirectory data directory, {@code s3://bucket/prefix}
     * @param settings S3 settings
     * @param client S3 client; the store closes it
     * @param nanoClock monotonic clock in nanoseconds
     */
    S3DataStore(String dataDirectory, S3Settings settings, S3Client client, LongSupplier nanoClock) {
        this(dataDirectory, settings, client, nanoClock, Thread::new, "-");
    }

    /**
     * Creates a store.
     *
     * @param dataDirectory data directory, {@code s3://bucket/prefix}
     * @param settings S3 settings
     * @param client S3 client; the store closes it
     * @param nanoClock monotonic clock in nanoseconds
     * @param threads creates the thread of background listings
     * @param node name of this server node, for logs
     */
    public S3DataStore(String dataDirectory, S3Settings settings, S3Client client,
            LongSupplier nanoClock, ThreadFactory threads, String node) {
        this(dataDirectory, settings, client, nanoClock, threads, node, true);
    }

    /**
     * Creates a store that may share its client with other stores (the lakefs plugin keeps one
     * store per pinned commit over one client).
     *
     * @param dataDirectory data directory, {@code s3://bucket/prefix}
     * @param settings S3 settings
     * @param client S3 client
     * @param nanoClock monotonic clock in nanoseconds
     * @param threads creates the thread of background listings
     * @param node name of this server node, for logs
     * @param ownsClient whether {@link #close()} closes the client
     */
    public S3DataStore(String dataDirectory, S3Settings settings, S3Client client,
            LongSupplier nanoClock, ThreadFactory threads, String node, boolean ownsClient) {
        this(dataDirectory, settings, client, nanoClock, threads, node, ownsClient, "");
    }

    /**
     * Creates a store of a named S3 configuration ({@code arrowflight.s3.stores}); its DuckDB
     * secret carries the name.
     *
     * @param dataDirectory data directory, {@code s3://bucket/prefix}
     * @param settings S3 settings
     * @param client S3 client
     * @param nanoClock monotonic clock in nanoseconds
     * @param threads creates the thread of background listings
     * @param node name of this server node, for logs
     * @param ownsClient whether {@link #close()} closes the client
     * @param configName store name, empty for the main configuration
     */
    public S3DataStore(String dataDirectory, S3Settings settings, S3Client client,
            LongSupplier nanoClock, ThreadFactory threads, String node, boolean ownsClient,
            String configName) {
        this.ownsClient = ownsClient;
        this.configName = Objects.requireNonNull(configName, "configName");
        this.root = S3Uri.parse(dataDirectory);
        this.settings = Objects.requireNonNull(settings, "settings");
        this.client = Objects.requireNonNull(client, "client");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.node = Objects.requireNonNull(node, "node");
        Objects.requireNonNull(threads, "threads");
        this.listingTtlNanos = TimeUnit.SECONDS.toNanos(settings.listingTtlSeconds());
        this.refresher = Executors.newSingleThreadExecutor(r -> {
            Thread t = threads.newThread(r);
            t.setName("s3-listing");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Builds the S3 client: static credentials, explicit region and endpoint, checksums only
     * where the API needs them, and an HTTP client that trusts only the configured CA file. The
     * client never reads profile files, instance metadata or AWS environment variables for
     * credentials, region or endpoint.
     *
     * @param settings S3 settings with credentials
     * @return S3 client
     */
    public static S3Client createClient(S3Settings settings) {
        if (System.getProperty(SdkSystemSetting.AWS_EC2_METADATA_DISABLED.property()) == null) {
            System.setProperty(SdkSystemSetting.AWS_EC2_METADATA_DISABLED.property(), "true");
        }
        AwsCredentials credentials = settings.sessionToken() == null
                ? AwsBasicCredentials.create(settings.accessKey(), settings.secretKey())
                : AwsSessionCredentials.create(settings.accessKey(), settings.secretKey(),
                        settings.sessionToken());
        ProfileFile noProfile = ProfileFile.builder().content("")
                .type(ProfileFile.Type.CONFIGURATION).build();
        return S3Client.builder()
                .region(Region.of(settings.region()))
                .endpointOverride(settings.endpointUri())
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .forcePathStyle(settings.pathStyle())
                .dualstackEnabled(false)
                .fipsEnabled(false)
                .defaultsMode(DefaultsMode.LEGACY)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .overrideConfiguration(o -> o.defaultProfileFile(noProfile)
                        .defaultProfileName("default"))
                .httpClient(httpClient(settings))
                .build();
    }

    /**
     * Builds the HTTP client of the S3 client.
     *
     * @param settings S3 settings
     * @return Apache HTTP client
     */
    private static SdkHttpClient httpClient(S3Settings settings) {
        ApacheHttpClient.Builder builder = ApacheHttpClient.builder()
                .maxConnections(settings.maxConnections())
                .connectionTimeout(Duration.ofSeconds(10))
                .socketTimeout(Duration.ofSeconds(60));
        if (settings.caFile() != null) {
            TrustManager[] trustManagers = trustManagers(Path.of(settings.caFile()));
            builder.tlsTrustManagersProvider(() -> trustManagers);
        }
        if (!settings.verifyTls()) {
            return builder.buildWithDefaults(AttributeMap.builder()
                    .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());
        }
        return builder.build();
    }

    /**
     * Builds trust managers that trust exactly the certificates of a PEM file.
     *
     * @param caFile PEM file with one or more certificates
     * @return trust managers
     * @throws IllegalArgumentException when the file holds no certificate or cannot be read
     */
    public static TrustManager[] trustManagers(Path caFile) {
        try (InputStream input = Files.newInputStream(caFile)) {
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509").generateCertificates(input);
            if (certificates.isEmpty()) {
                throw new IllegalArgumentException("s3CaFile holds no certificate: " + caFile);
            }
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            int i = 0;
            for (Certificate certificate : certificates) {
                keyStore.setCertificateEntry("ca-" + i++, certificate);
            }
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(keyStore);
            return factory.getTrustManagers();
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("Cannot read s3CaFile " + caFile + ": " + e.getMessage(), e);
        }
    }

    @Override
    public String kind() {
        return "s3";
    }

    @Override
    public boolean shared() {
        return true;
    }

    @Override
    public boolean hasLocality() {
        return false;
    }

    @Override
    public String root() {
        return root.uri();
    }

    @Override
    public boolean exists(String relativePath) throws IOException {
        Snapshot current = snapshot();
        String path = normalize(relativePath);
        if (path.isEmpty()) {
            return !current.files().isEmpty() || !current.directories().isEmpty();
        }
        return current.directories().contains(path) || current.files().containsKey(path);
    }

    @Override
    public Map<String, String> directories(String relativePath) throws IOException {
        Snapshot current = snapshot();
        String path = normalize(relativePath);
        if (!path.isEmpty() && !current.directories().contains(path)) {
            throw new FileNotFoundException("No such directory: " + root.objectUri(root.key(path)));
        }
        String start = path.isEmpty() ? "" : path + "/";
        Map<String, String> result = new LinkedHashMap<>();
        for (String directory : childrenOf(current.directories(), start)) {
            String name = directory.substring(start.length());
            if (name.indexOf('/') < 0) {
                result.put(name, root.objectUri(root.key(directory)));
            }
        }
        return result;
    }

    @Override
    public List<DataFile> parquetFiles(String relativeDirectory) throws IOException {
        Snapshot current = snapshot();
        String path = normalize(relativeDirectory);
        if (!path.isEmpty() && !current.directories().contains(path)) {
            throw new FileNotFoundException("No such directory: " + root.objectUri(root.key(path)));
        }
        String start = path.isEmpty() ? "" : path + "/";
        NavigableMap<String, DataFile> below = start.isEmpty()
                ? current.files() : current.files().subMap(start, true, upperBound(start), false);
        List<DataFile> result = new ArrayList<>();
        for (DataFile file : below.values()) {
            if (isParquet(file.relativePath())) {
                result.add(file);
            }
        }
        return result;
    }

    @Override
    public List<DataFile> files(String relativeDirectory, boolean recursive) throws IOException {
        Snapshot current = snapshot();
        String path = normalize(relativeDirectory);
        if (!path.isEmpty() && !current.directories().contains(path)) {
            throw new FileNotFoundException("No such directory: " + root.objectUri(root.key(path)));
        }
        String start = path.isEmpty() ? "" : path + "/";
        NavigableMap<String, DataFile> below = start.isEmpty()
                ? current.files() : current.files().subMap(start, true, upperBound(start), false);
        List<DataFile> result = new ArrayList<>();
        for (DataFile file : below.values()) {
            String rest = file.relativePath().substring(start.length());
            if ((recursive || rest.indexOf('/') < 0) && !DataStore.hiddenPath(rest)) {
                result.add(file);
            }
        }
        return result;
    }

    @Override
    public DataFile file(String relativePath) throws IOException {
        String key = root.key(relativePath);
        DataFile listed = snapshot().files().get(normalize(relativePath));
        if (listed != null) {
            return listed;
        }
        // Newer than the listing: look it up.
        try {
            HeadObjectResponse head = client.headObject(r -> r.bucket(root.bucket()).key(key));
            long modified = head.lastModified() == null ? 0L : head.lastModified().toEpochMilli();
            return new DataFile(normalize(relativePath), root.objectUri(key),
                    head.contentLength(), modified, Set.of());
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("No such file: " + root.objectUri(key));
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                throw new FileNotFoundException("No such file: " + root.objectUri(key));
            }
            throw storageError("look up " + root.objectUri(key), e);
        } catch (SdkException e) {
            throw storageError("look up " + root.objectUri(key), e);
        }
    }

    @Override
    public ReadableFile open(String relativePath) throws IOException {
        return open(file(relativePath));
    }

    @Override
    public ReadableFile open(DataFile file) {
        String key = root.key(file.relativePath());
        return new S3ReadableFile((start, count) -> readRange(key, start, count),
                root.objectUri(key), file.size());
    }

    @Override
    public String duckDbUri(String relativePath) {
        return root.objectUri(root.key(relativePath));
    }

    @Override
    public String duckDbUri(DataFile file) {
        return file.location();
    }

    @Override
    public List<String> duckDbRoots() {
        return List.of(root.root());
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        contribute(setup, settings, root, secretName(configName));
    }

    @Override
    public String configName() {
        return configName;
    }

    /**
     * Names the DuckDB secret of a configuration.
     *
     * @param configName store name, empty for the main configuration
     * @return {@code flight_s3}, or {@code flight_s3_<name>} for a named store
     */
    static String secretName(String configName) {
        return configName.isEmpty() ? DUCKDB_SECRET_NAME : DUCKDB_SECRET_NAME + "_" + configName;
    }

    @Override
    public Map<String, Long> localInventory(String host) {
        // No file is closer to one node than to another; the planner reads the listing instead.
        return Map.of();
    }

    @Override
    public void close() {
        refresher.shutdownNow();
        if (ownsClient) {
            client.close();
        }
    }

    /**
     * Describes what lets a DuckDB connection read the data directory and nothing else on S3.
     * The settings httpfs fills from AWS_* and DUCKDB_S3_* environment variables are blanked;
     * the credentials go into a temporary secret scoped to the data directory, never into a
     * setting, because settings can be read back; query parameters on s3:// URLs, which could
     * point a read at another endpoint, are switched off. DuckDB retries failed requests,
     * connect errors included, as the settings say.
     *
     * @param setup receives the extension, settings and secret, in this order
     * @param settings S3 settings with credentials
     * @param root data directory
     */
    public static void contribute(DuckDbSetup setup, S3Settings settings, S3Uri root) {
        contribute(setup, settings, root, DUCKDB_SECRET_NAME);
    }

    /**
     * Describes the DuckDB setup of one S3 store, its secret under the given name; see
     * {@link #contribute(DuckDbSetup, S3Settings, S3Uri)}.
     *
     * @param setup receives the extension, settings and secret, in this order
     * @param settings S3 settings with credentials
     * @param root data directory
     * @param secretName name of the DuckDB secret
     */
    static void contribute(DuckDbSetup setup, S3Settings settings, S3Uri root, String secretName) {
        setup.loadExtension("httpfs");
        for (String setting : List.of("s3_access_key_id", "s3_secret_access_key",
                "s3_session_token", "s3_endpoint", "s3_region")) {
            setup.set(setting, "");
        }
        setup.set("s3_url_compatibility_mode", true);
        setup.set("enable_http_logging", false);
        setup.set("http_retries", (long) settings.httpRetries());
        setup.set("http_retry_wait_ms", (long) settings.httpRetryWaitMs());
        setup.set("http_retry_backoff", (long) settings.httpRetryBackoff());
        if (settings.useSsl()) {
            setup.set("enable_server_cert_verification", settings.verifyTls());
            if (settings.caFile() != null) {
                setup.set("ca_cert_file", settings.caFile());
            }
        }
        if (settings.metadataCache()) {
            setup.set("parquet_metadata_cache", true);
            setup.set("enable_http_metadata_cache", true);
        }
        Map<String, SecretValue> options = new LinkedHashMap<>();
        if (settings.hasCredentials()) {
            options.put("KEY_ID", SecretValue.plain(settings.accessKey()));
            options.put("SECRET", SecretValue.sensitive(settings.secretKey()));
            if (settings.sessionToken() != null) {
                options.put("SESSION_TOKEN", SecretValue.sensitive(settings.sessionToken()));
            }
        }
        options.put("REGION", SecretValue.plain(settings.region()));
        options.put("ENDPOINT", SecretValue.plain(settings.duckDbEndpoint()));
        options.put("URL_STYLE", SecretValue.plain(settings.urlStyle()));
        options.put("USE_SSL", SecretValue.of(settings.useSsl()));
        options.put("URL_COMPATIBILITY_MODE", SecretValue.of(true));
        setup.createSecret(secretName, "s3", root.root(), options);
    }

    /**
     * Returns the current listing, listing the data directory first if this is the first use
     * and starting a background refresh when the listing has expired.
     *
     * @return listing
     * @throws IOException when the first listing fails
     */
    private Snapshot snapshot() throws IOException {
        Snapshot current = snapshot;
        if (current == null) {
            synchronized (initialListing) {
                current = snapshot;
                if (current == null) {
                    current = list();
                    snapshot = current;
                }
                return current;
            }
        }
        if (listingTtlNanos == 0) {
            current = list();
            snapshot = current;
            return current;
        }
        if (nanoClock.getAsLong() - current.loadedAtNanos() >= listingTtlNanos
                && refreshing.compareAndSet(false, true)) {
            try {
                refresher.execute(this::refresh);
            } catch (RuntimeException e) {
                refreshing.set(false);
                LOGGER.warn("node={} s3=refreshNotStarted error={}", node, e.toString());
            }
        }
        return current;
    }

    /**
     * Replaces the listing with a new one; on failure the old one stays and the next use tries
     * again.
     */
    private void refresh() {
        try {
            snapshot = list();
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("node={} s3=listingRefreshFailed dataDir={} error={}",
                    node, root.uri(), settings.redact(e.getMessage()));
        } finally {
            refreshing.set(false);
        }
    }

    /**
     * Lists every object below the data directory.
     *
     * @return new listing
     * @throws IOException on storage access failure
     */
    private Snapshot list() throws IOException {
        long t = TIMING_LOG.isDebugEnabled() ? System.nanoTime() : -1L;
        long started = nanoClock.getAsLong();
        TreeMap<String, DataFile> files = new TreeMap<>();
        TreeSet<String> directories = new TreeSet<>();
        int pages = 0;
        int skipped = 0;
        String token = null;
        try {
            do {
                ListObjectsV2Request.Builder request = ListObjectsV2Request.builder()
                        .bucket(root.bucket()).maxKeys(LIST_PAGE_SIZE);
                if (!root.prefix().isEmpty()) {
                    request.prefix(root.prefix());
                }
                if (token != null) {
                    request.continuationToken(token);
                }
                ListObjectsV2Response page = client.listObjectsV2(request.build());
                pages++;
                for (S3Object object : page.contents()) {
                    if (!add(object, files, directories)) {
                        skipped++;
                    }
                }
                token = Boolean.TRUE.equals(page.isTruncated()) ? page.nextContinuationToken() : null;
            } while (token != null);
        } catch (SdkException e) {
            throw storageError("list " + root.root(), e);
        }
        if (skipped > 0) {
            LOGGER.warn("node={} s3=keysSkipped count={} dataDir={} (keys with characters a"
                    + " data path may not have)", node, skipped, root.uri());
        }
        if (t >= 0) {
            TIMING_LOG.debug("qid=null node={} TIMING thread={} durationUs={} tag=s3.list files={}"
                    + " directories={} pages={}", node, Thread.currentThread().getName(),
                    TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t), files.size(),
                    directories.size(), pages);
        }
        return new Snapshot(started, Collections.unmodifiableNavigableMap(files),
                Collections.unmodifiableNavigableSet(directories));
    }

    /**
     * Adds one listed object to a listing, with the directories above it.
     *
     * @param object listed object
     * @param files files of the listing
     * @param directories directories of the listing
     * @return false when the key cannot be named as a data path and was left out
     */
    private boolean add(S3Object object, Map<String, DataFile> files, Set<String> directories) {
        String key = object.key();
        if (key == null || !key.startsWith(root.prefix())) {
            return false;
        }
        String relative = key.substring(root.prefix().length());
        if (relative.isEmpty()) {
            return true;
        }
        boolean marker = relative.endsWith("/");
        String path = marker ? relative.substring(0, relative.length() - 1) : relative;
        try {
            root.key(path);
        } catch (IllegalArgumentException e) {
            return false;
        }
        String[] segments = path.split("/");
        StringBuilder directory = new StringBuilder();
        int parents = marker ? segments.length : segments.length - 1;
        for (int i = 0; i < parents; i++) {
            if (i > 0) {
                directory.append('/');
            }
            directory.append(segments[i]);
            directories.add(directory.toString());
        }
        if (!marker) {
            long modified = object.lastModified() == null ? 0L : object.lastModified().toEpochMilli();
            long size = object.size() == null ? 0L : object.size();
            files.put(path, new DataFile(path, root.objectUri(key), size, modified, Set.of()));
        }
        return true;
    }

    /**
     * Reads a byte range of an object.
     *
     * @param key object key
     * @param start first byte
     * @param count number of bytes
     * @return the bytes
     * @throws IOException on storage access failure
     */
    private byte[] readRange(String key, long start, int count) throws IOException {
        long last = start + count - 1;
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(root.bucket()).key(key)
                    .range("bytes=" + start + "-" + last).build();
            byte[] bytes = client.getObjectAsBytes(request).asByteArray();
            if (bytes.length > count && bytes.length > last) {
                // A store that ignores Range sends the whole object.
                return Arrays.copyOfRange(bytes, (int) start, (int) start + count);
            }
            return bytes;
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("No such file: " + root.objectUri(key));
        } catch (SdkException e) {
            throw storageError("read " + root.objectUri(key), e);
        }
    }

    /**
     * Wraps a client failure, without anything that could quote the credentials.
     *
     * @param action what was being done
     * @param e client failure
     * @return I/O exception
     */
    private IOException storageError(String action, SdkException e) {
        return new IOException("S3: cannot " + action + ": " + settings.redact(e.getMessage()), e);
    }

    /**
     * Lists the directories that start with a prefix.
     *
     * @param directories all directories
     * @param start prefix, empty or ending with "/"
     * @return matching directories in order
     */
    private static NavigableSet<String> childrenOf(NavigableSet<String> directories, String start) {
        return start.isEmpty()
                ? directories : directories.subSet(start, true, upperBound(start), false);
    }

    /**
     * Returns the smallest string after every string that starts with a directory prefix: the
     * prefix with its "/" replaced by the next character, "0".
     *
     * @param start directory prefix ending with "/"
     * @return exclusive upper bound
     */
    private static String upperBound(String start) {
        return start.substring(0, start.length() - 1) + (char) ('/' + 1);
    }

    /**
     * Drops a trailing "/" from a relative path.
     *
     * @param relativePath relative path or null
     * @return path without trailing "/", empty for the data directory
     */
    private static String normalize(String relativePath) {
        if (relativePath == null) {
            return "";
        }
        String path = relativePath;
        while (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * Tells whether a path names a Parquet file; the extension is matched in any case.
     *
     * @param path relative path
     * @return true for a name ending with ".parquet"
     */
    private static boolean isParquet(String path) {
        return path.toLowerCase(Locale.ROOT).endsWith(PARQUET_EXTENSION);
    }
}
