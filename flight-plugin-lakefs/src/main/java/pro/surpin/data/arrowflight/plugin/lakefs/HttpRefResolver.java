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
        URI uri = api.resolve("repositories/" + encode(repository) + "/branches/" + encode(ref));
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
            throw new IOException("Interrupted while resolving lakeFS branch " + repository + "/"
                    + ref, e);
        }
        int status = response.statusCode();
        if (status == 404) {
            return Optional.empty();
        }
        if (status != 200) {
            throw new IOException("lakeFS API GET " + uri + " returned HTTP " + status);
        }
        Matcher matcher = COMMIT_ID.matcher(response.body());
        if (!matcher.find()) {
            throw new IOException("lakeFS API GET " + uri + " returned no commit_id");
        }
        return Optional.of(matcher.group(1));
    }

    private static String encode(String part) {
        return URLEncoder.encode(part, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
