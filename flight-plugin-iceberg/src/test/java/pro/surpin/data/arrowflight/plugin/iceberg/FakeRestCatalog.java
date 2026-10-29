package pro.surpin.data.arrowflight.plugin.iceberg;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * A small Iceberg REST catalog over a HadoopCatalog warehouse (or any namespace-aware catalog), answering the read endpoints of
 * the REST spec the way Gravitino and Polaris do: GET /v1/config (with prefix "wh1"),
 * namespaces, tables (two pages), loadTable (metadata-location + metadata), and an OAuth2
 * client_credentials token endpoint. Requests need "Authorization: Bearer token-1".
 */
public final class FakeRestCatalog implements AutoCloseable {

    /** Client credential the token endpoint accepts. */
    public static final String CREDENTIAL = "client-1:secret-1";
    private static final String TOKEN = "token-1";
    private static final String PREFIX = "wh1";

    private final HttpServer server;
    private final Catalog catalog;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger tokens = new AtomicInteger();
    /** Credentials vended so far; each loadTable that asks gets a new one. */
    private final AtomicInteger vended = new AtomicInteger();
    /** Lifetime of vended credentials in ms; 0 vends none. */
    private volatile long vendMillis;
    /** Prefix of vended credentials; null for the table location. */
    private volatile String vendPrefix;
    /** Further S3FileIO properties of vended credentials (endpoint, path style), as JSON members. */
    private volatile String vendExtra = "";

    /**
     * Starts the catalog on a free port of the loopback interface.
     *
     * @param warehouse warehouse directory written by {@link IcebergFixtures}
     * @throws IOException when the port cannot be opened
     */
    public FakeRestCatalog(String warehouse) throws IOException {
        this((Catalog) IcebergFixtures.catalog(warehouse));
    }

    /**
     * Starts the catalog over another catalog on a free port of the loopback interface.
     *
     * @param catalog catalog that also supports namespaces
     * @throws IOException when the port cannot be opened
     */
    public FakeRestCatalog(Catalog catalog) throws IOException {
        this.catalog = catalog;
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    /**
     * Starts a catalog; for callers in another class loader.
     *
     * @param warehouse warehouse directory
     * @return the catalog, which is AutoCloseable
     * @throws IOException when the port cannot be opened
     */
    public static FakeRestCatalog start(String warehouse) throws IOException {
        return new FakeRestCatalog(warehouse);
    }

    /**
     * Returns the catalog URI.
     *
     * @return http://127.0.0.1:port
     */
    public String uri() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /**
     * Lists the request paths seen.
     *
     * @return method and path of every request
     */
    public List<String> requests() {
        return new ArrayList<>(requests);
    }

    /**
     * Counts the tokens handed out.
     *
     * @return number of token requests answered
     */
    public int tokens() {
        return tokens.get();
    }

    /**
     * Vends S3 credentials (key {@code AKIA<n>}, token {@code token-<n>}, with an expiry) in
     * {@code storage-credentials} to every loadTable that asks with
     * {@code X-Iceberg-Access-Delegation: vended-credentials}.
     *
     * @param lifetimeMillis how long each credential is valid; 0 stops vending
     * @param prefix prefix the credentials name, or null for the table location
     */
    public void vendCredentials(long lifetimeMillis, String prefix) {
        this.vendPrefix = prefix;
        this.vendMillis = lifetimeMillis;
    }

    /**
     * Adds S3FileIO properties to every vended credential's config.
     *
     * @param endpoint {@code s3.endpoint}
     * @param pathStyle {@code s3.path-style-access}
     */
    public void vendS3Endpoint(String endpoint, boolean pathStyle) {
        this.vendExtra = ",\"s3.endpoint\":\"" + endpoint + "\",\"s3.path-style-access\":\""
                + pathStyle + "\"";
    }

    /**
     * Counts the credentials vended.
     *
     * @return number of loadTable answers that carried credentials
     */
    public int vended() {
        return vended.get();
    }

    @Override
    public void close() throws IOException {
        server.stop(0);
        if (catalog instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getRawPath();
            String query = exchange.getRequestURI().getRawQuery();
            requests.add(exchange.getRequestMethod() + " " + path + (query == null ? "" : "?" + query));
            if (path.equals("/v1/oauth/tokens")) {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                if (body.contains("grant_type=client_credentials") && body.contains("client_id=client-1")
                        && body.contains("client_secret=secret-1")) {
                    tokens.incrementAndGet();
                    send(exchange, 200, "{\"access_token\":\"" + TOKEN + "\",\"token_type\":\"bearer\","
                            + "\"expires_in\":3600}");
                } else {
                    send(exchange, 401, "{\"error\":\"invalid_client\",\"error_description\":\"bad\"}");
                }
                return;
            }
            if (!("Bearer " + TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                send(exchange, 401, error(401, "NotAuthorizedException", "no token"));
                return;
            }
            if (path.equals("/v1/config")) {
                send(exchange, 200, "{\"defaults\":{},\"overrides\":{\"prefix\":\"" + PREFIX + "\"}}");
                return;
            }
            String base = "/v1/" + PREFIX + "/";
            if (!path.startsWith(base)) {
                send(exchange, 404, error(404, "NoSuchResourceException", path));
                return;
            }
            String[] parts = path.substring(base.length()).split("/");
            if (parts.length == 1 && parts[0].equals("namespaces")) {
                StringBuilder json = new StringBuilder("{\"namespaces\":[");
                List<Namespace> namespaces = ((SupportsNamespaces) catalog).listNamespaces();
                for (int i = 0; i < namespaces.size(); i++) {
                    json.append(i > 0 ? "," : "").append("[\"").append(namespaces.get(i).level(0)).append("\"]");
                }
                send(exchange, 200, json.append("]}").toString());
                return;
            }
            String namespace = parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            if (parts.length == 3 && parts[2].equals("tables")) {
                List<TableIdentifier> ids = catalog.listTables(Namespace.of(namespace));
                // Two pages: the client must follow next-page-token.
                boolean second = query != null && query.contains("pageToken=p2");
                int half = ids.size() / 2;
                List<TableIdentifier> page = second ? ids.subList(half, ids.size()) : ids.subList(0, half);
                StringBuilder json = new StringBuilder("{\"identifiers\":[");
                for (int i = 0; i < page.size(); i++) {
                    json.append(i > 0 ? "," : "").append("{\"namespace\":[\"").append(namespace)
                            .append("\"],\"name\":\"").append(page.get(i).name()).append("\"}");
                }
                json.append("]").append(second ? "" : ",\"next-page-token\":\"p2\"").append("}");
                send(exchange, 200, json.toString());
                return;
            }
            if (parts.length == 4 && parts[2].equals("tables")) {
                String name = URLDecoder.decode(parts[3], StandardCharsets.UTF_8);
                Table table;
                try {
                    table = catalog.loadTable(TableIdentifier.of(namespace, name));
                } catch (NoSuchTableException e) {
                    send(exchange, 404, error(404, "NoSuchTableException", "Table does not exist: " + name));
                    return;
                }
                TableMetadata metadata = ((HasTableOperations) table).operations().current();
                String credentials = "";
                long lifetime = vendMillis;
                if (lifetime > 0 && "vended-credentials".equals(
                        exchange.getRequestHeaders().getFirst("X-Iceberg-Access-Delegation"))) {
                    int n = vended.incrementAndGet();
                    String prefix = vendPrefix == null ? metadata.location() : vendPrefix;
                    credentials = ",\"storage-credentials\":[{\"prefix\":\"" + prefix
                            + "\",\"config\":{\"s3.access-key-id\":\"AKIA" + n
                            + "\",\"s3.secret-access-key\":\"secret-" + n
                            + "\",\"s3.session-token\":\"token-" + n
                            + "\",\"s3.session-token-expires-at-ms\":\""
                            + (System.currentTimeMillis() + lifetime)
                            + "\",\"client.region\":\"us-east-1\"" + vendExtra + "}}]";
                }
                send(exchange, 200, "{\"metadata-location\":\"" + metadata.metadataFileLocation()
                        + "\",\"metadata\":" + TableMetadataParser.toJson(metadata) + ",\"config\":{}"
                        + credentials + "}");
                return;
            }
            send(exchange, 404, error(404, "NoSuchResourceException", path));
        } catch (RuntimeException e) {
            send(exchange, 500, error(500, "ServerError", String.valueOf(e)));
        }
    }

    private static String error(int code, String type, String message) {
        return "{\"error\":{\"message\":\"" + message.replace("\"", "'") + "\",\"type\":\"" + type
                + "\",\"code\":" + code + "}}";
    }

    private static void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
