package pro.surpin.data.arrowflight.plugin.lakefs;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import pro.surpin.data.arrowflight.plugin.s3.S3DataStore;

/**
 * Asks the lakeFS REST API for a branch head: {@code GET <api>/repositories/{repo}/branches/{ref}}
 * with HTTP basic authentication by the lakeFS access key. Uses the JDK HTTP client only.
 */
public final class HttpRefResolver implements RefResolver {

    private static final Pattern COMMIT_ID =
            Pattern.compile("\"commit_id\"\\s*:\\s*\"([A-Za-z0-9_.-]{1,256})\"");

    private static final Pattern ID =
            Pattern.compile("\"id\"\\s*:\\s*\"([A-Za-z0-9_.-]{1,256})\"");

    private final URI api;
    private final String authorization;
    private final HttpClient client;
    private final Duration timeout;

    /**
     * Creates a resolver.
     *
     * @param api API base, for example {@code http://lakefs:8000/api/v1}
     * @param accessKey lakeFS access key id
     * @param secretKey lakeFS secret access key
     * @param caFile PEM file with the only CAs trusted for TLS, null for the JDK's
     * @param timeout connect and request timeout
     */
    public HttpRefResolver(URI api, String accessKey, String secretKey, String caFile,
            Duration timeout) {
        String base = Objects.requireNonNull(api, "api").toString();
        this.api = URI.create(base.endsWith("/") ? base : base + "/");
        this.authorization = accessKey == null || secretKey == null ? null
                : "Basic " + Base64.getEncoder().encodeToString(
                        (accessKey + ":" + secretKey).getBytes(StandardCharsets.UTF_8));
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(timeout)
                .followRedirects(HttpClient.Redirect.NEVER);
        if (caFile != null) {
            try {
                SSLContext tls = SSLContext.getInstance("TLS");
                tls.init(null, S3DataStore.trustManagers(Path.of(caFile)), null);
                builder.sslContext(tls);
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Cannot use caFile " + caFile
                        + " for the lakeFS API", e);
            }
        }
        this.client = builder.build();
    }

    @Override
    public Optional<String> branchHead(String repository, String ref) throws IOException {
        HttpResponse<String> response = get("repositories/" + encode(repository) + "/branches/"
                + encode(ref), repository, ref);
        if (response.statusCode() == 404) {
            return Optional.empty();
        }
        return Optional.of(commitId(response, COMMIT_ID));
    }

    @Override
    public String refCommit(String repository, String ref) throws IOException {
        HttpResponse<String> response = get("repositories/" + encode(repository) + "/commits/"
                + encode(ref), repository, ref);
        if (response.statusCode() == 404) {
            HttpResponse<String> repo = get("repositories/" + encode(repository), repository,
                    ref);
            throw new IOException(repo.statusCode() == 404
                    ? "lakeFS repository '" + repository + "' does not exist (" + api + ")"
                    : "lakeFS repository '" + repository + "' has no branch, tag or commit '"
                            + ref + "'");
        }
        return commitId(response, ID);
    }

    /**
     * Sends one GET; 200 and 404 are returned, anything else is an error that names the call but
     * neither the response body nor the credentials.
     */
    private HttpResponse<String> get(String path, String repository, String ref)
            throws IOException {
        URI uri = api.resolve(path);
        HttpRequest.Builder request = HttpRequest.newBuilder(uri).timeout(timeout)
                .header("Accept", "application/json").GET();
        if (authorization != null) {
            request.header("Authorization", authorization);
        }
        HttpResponse<String> response;
        try {
            response = client.send(request.build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while resolving lakeFS ref " + repository + "/"
                    + ref, e);
        } catch (IOException e) {
            // ConnectException and HttpTimeoutException often carry no message of their own.
            throw new IOException("lakeFS API GET " + uri + " failed, lakeFS not reachable: "
                    + e.getClass().getSimpleName()
                    + (e.getMessage() == null ? "" : " " + e.getMessage()), e);
        }
        int status = response.statusCode();
        if (status == 401 || status == 403) {
            throw new IOException("lakeFS API GET " + uri + " returned HTTP " + status
                    + ": lakeFS refused the credentials (FLIGHT_LAKEFS_ACCESS_KEY /"
                    + " FLIGHT_LAKEFS_SECRET_KEY or lakefs.credentialsFile)");
        }
        if (status != 200 && status != 404) {
            throw new IOException("lakeFS API GET " + uri + " returned HTTP " + status);
        }
        return response;
    }

    private static String commitId(HttpResponse<String> response, Pattern field)
            throws IOException {
        Matcher matcher = field.matcher(response.body());
        if (!matcher.find()) {
            throw new IOException("lakeFS API GET " + response.uri() + " returned no commit id");
        }
        return matcher.group(1);
    }

    private static String encode(String part) {
        return URLEncoder.encode(part, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
