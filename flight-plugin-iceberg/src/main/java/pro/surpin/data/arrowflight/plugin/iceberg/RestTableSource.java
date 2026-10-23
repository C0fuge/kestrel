package pro.surpin.data.arrowflight.plugin.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tables of an Iceberg REST catalog (the REST catalog API of the Iceberg spec: Gravitino's Iceberg
 * REST service, Polaris, Nessie, ...), read with the JDK HTTP client: GET /v1/config, the
 * namespace and table lists, and loadTable for a table's metadata location and metadata. Only
 * reads; no credentials are vended to DuckDB (it reads the files with the server's own storage
 * setup). Authentication: a bearer token, or an OAuth2 client credential exchanged for a token.
 */
final class RestTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestTableSource.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    /** Separator of namespace levels in REST paths. */
    private static final String UNIT_SEPARATOR = "\u001f";
    /** Renew an OAuth2 token this long before it expires. */
    private static final long TOKEN_MARGIN_MILLIS = 60_000;

    private final IcebergSettings settings;
    private final HttpClient http;
    private final String base;
    private String accessToken;
    private long tokenExpiresAtMillis;

    /**
     * Connects to the catalog and reads its configuration.
     *
     * @param settings catalog settings
     * @param http HTTP client
     * @throws IOException when the catalog cannot be reached
     */
    RestTableSource(IcebergSettings settings, HttpClient http) throws IOException {
        this.settings = settings;
        this.http = http;
        String prefix = settings.prefix();
        String configUri = settings.uri() + "/v1/config" + (settings.warehouse().isBlank() ? ""
                : "?warehouse=" + encode(settings.warehouse()));
        JsonNode config = get(configUri).orElseThrow(
                () -> new IOException("REST catalog " + settings.uri() + " has no /v1/config"));
        if (prefix.isBlank()) {
            prefix = text(config.path("overrides").path("prefix"))
                    .or(() -> text(config.path("defaults").path("prefix"))).orElse("");
        }
        this.base = settings.uri() + "/v1/" + (prefix.isBlank() ? "" : prefix + "/");
        LOGGER.info("iceberg=restCatalog uri={} warehouse={} prefix={}", settings.uri(),
                settings.warehouse(), prefix);
    }

    @Override
    public List<String> namespaces() throws IOException {
        List<String> result = new ArrayList<>();
        for (JsonNode page : pages(base + "namespaces")) {
            for (JsonNode namespace : page.path("namespaces")) {
                if (namespace.size() == 1) {
                    result.add(namespace.get(0).asText());
                } else {
                    LOGGER.debug("iceberg=namespaceSkipped namespace={} reason=multiLevel", namespace);
                }
            }
        }
        return result;
    }

    @Override
    public List<String> tables(String namespace) throws IOException {
        List<String> result = new ArrayList<>();
        for (JsonNode page : pages(base + "namespaces/" + encode(namespace) + "/tables")) {
            for (JsonNode id : page.path("identifiers")) {
                result.add(id.path("name").asText());
            }
        }
        return result;
    }

    @Override
    public Optional<Loaded> load(String namespace, String table) throws IOException {
        Optional<JsonNode> response = get(base + "namespaces/" + encode(namespace) + "/tables/"
                + encode(table));
        if (response.isEmpty()) {
            return Optional.empty();
        }
        JsonNode body = response.get();
        String location = text(body.path("metadata-location")).orElseThrow(() -> new IOException(
                "REST catalog sent no metadata-location for " + namespace + "." + table));
        TableMetadata metadata = TableMetadataParser.fromJson(location, body.path("metadata"));
        return Optional.of(new Loaded(location, metadata));
    }

    /**
     * Reads every page of a list.
     *
     * @param uri list URI
     * @return pages
     * @throws IOException on HTTP failure
     */
    private List<JsonNode> pages(String uri) throws IOException {
        List<JsonNode> pages = new ArrayList<>();
        String token = null;
        do {
            String pageUri = token == null ? uri : uri + "?pageToken=" + encode(token);
            JsonNode page = get(pageUri).orElseThrow(() -> new IOException("Not found: " + uri));
            pages.add(page);
            token = text(page.path("next-page-token")).orElse(null);
        } while (token != null && pages.size() < 10_000);
        return pages;
    }

    /**
     * Sends a GET request.
     *
     * @param uri request URI
     * @return JSON body, empty on 404
     * @throws IOException on another status or a transport failure
     */
    private Optional<JsonNode> get(String uri) throws IOException {
        HttpResponse<String> response = send(uri, false);
        if (response.statusCode() == 401 && settings.credential().isPresent()) {
            // The token may have been revoked before it expired: get a new one, once.
            synchronized (this) {
                accessToken = null;
            }
            response = send(uri, true);
        }
        int status = response.statusCode();
        if (status == 404) {
            return Optional.empty();
        }
        if (status / 100 != 2) {
            throw new IOException("REST catalog answered " + status + " to GET " + uri + ": "
                    + errorMessage(response.body()));
        }
        return Optional.of(JSON.readTree(response.body()));
    }

    /**
     * Sends a GET request with the authorization header.
     *
     * @param uri request URI
     * @param retry whether this is the retry after a 401
     * @return response
     * @throws IOException on transport failure
     */
    private HttpResponse<String> send(String uri, boolean retry) throws IOException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(uri))
                .timeout(Duration.ofMillis(settings.requestTimeoutMillis()))
                .header("Accept", "application/json").GET();
        authorization().ifPresent(value -> request.header("Authorization", value));
        try {
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while calling the REST catalog" + (retry ? " again" : ""), e);
        } catch (IOException e) {
            throw unreachable(settings.uri(), e);
        }
    }

    /**
     * Wraps a transport failure so that its message names the service: the JDK client's own
     * exceptions often have none (a refused connection is a ConnectException without a message,
     * which reached clients as "... cannot be read: null").
     *
     * @param what service, for example {@code http://host:9001/iceberg}
     * @param e transport failure
     * @return exception to throw
     */
    static IOException unreachable(String what, IOException e) {
        Throwable cause = e;
        while (cause.getMessage() == null && cause.getCause() != null) {
            cause = cause.getCause();
        }
        String detail = cause.getClass().getSimpleName()
                + (cause.getMessage() == null ? "" : ": " + cause.getMessage());
        return new IOException("Iceberg REST catalog unavailable: " + what + " cannot be reached ("
                + detail + ")", e);
    }

    /**
     * Returns the authorization header value.
     *
     * @return header value, empty without credentials
     * @throws IOException when a token cannot be obtained
     */
    private synchronized Optional<String> authorization() throws IOException {
        if (settings.token().isPresent()) {
            return Optional.of("Bearer " + settings.token().get());
        }
        if (settings.credential().isEmpty()) {
            return Optional.empty();
        }
        long now = System.currentTimeMillis();
        if (accessToken == null || now >= tokenExpiresAtMillis - TOKEN_MARGIN_MILLIS) {
            fetchToken(now);
        }
        return Optional.of("Bearer " + accessToken);
    }

    /**
     * Exchanges the client credential for a token (OAuth2 client_credentials).
     *
     * @param now current time
     * @throws IOException when the token endpoint refuses
     */
    private void fetchToken(long now) throws IOException {
        String credential = settings.credential().orElseThrow();
        int colon = credential.indexOf(':');
        String clientId = colon < 0 ? "" : credential.substring(0, colon);
        String secret = colon < 0 ? credential : credential.substring(colon + 1);
        String form = "grant_type=client_credentials"
                + (clientId.isEmpty() ? "" : "&client_id=" + encode(clientId))
                + "&client_secret=" + encode(secret) + "&scope=" + encode(settings.scope());
        String endpoint = settings.oauthUri().isBlank() ? settings.uri() + "/v1/oauth/tokens"
                : settings.oauthUri();
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
                .timeout(Duration.ofMillis(settings.requestTimeoutMillis()))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(form)).build();
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while getting an OAuth2 token", e);
        } catch (IOException e) {
            throw unreachable("OAuth2 token endpoint " + endpoint, e);
        }
        if (response.statusCode() / 100 != 2) {
            throw new IOException("OAuth2 token endpoint " + endpoint + " answered "
                    + response.statusCode() + ": " + errorMessage(response.body()));
        }
        JsonNode body = JSON.readTree(response.body());
        accessToken = text(body.path("access_token")).orElseThrow(
                () -> new IOException("OAuth2 token endpoint " + endpoint + " sent no access_token"));
        long expiresIn = body.path("expires_in").asLong(3600);
        tokenExpiresAtMillis = now + expiresIn * 1000L;
    }

    /**
     * Extracts the message of an error response.
     *
     * @param body response body
     * @return message, at most 300 characters
     */
    private static String errorMessage(String body) {
        String message = body == null ? "" : body;
        try {
            JsonNode node = JSON.readTree(message);
            JsonNode error = node.path("error");
            if (error.isObject()) {
                message = error.path("type").asText("") + " " + error.path("message").asText("");
            } else if (node.has("error_description")) {
                message = node.path("error").asText("") + " " + node.path("error_description").asText("");
            }
        } catch (IOException | RuntimeException e) {
            // Not JSON: keep the text.
        }
        message = message.strip();
        return message.length() > 300 ? message.substring(0, 300) + "..." : message;
    }

    /**
     * Reads a non-blank text value.
     *
     * @param node JSON node
     * @return text, empty when missing, null or blank
     */
    private static Optional<String> text(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return Optional.empty();
        }
        String value = node.asText();
        return value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    /**
     * Encodes a path segment or query value.
     *
     * @param value value
     * @return encoded value
     */
    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")
                .replace(URLEncoder.encode(UNIT_SEPARATOR, StandardCharsets.UTF_8), "%1F");
    }
}
