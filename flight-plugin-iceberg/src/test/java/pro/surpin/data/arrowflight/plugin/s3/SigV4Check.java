package pro.surpin.data.arrowflight.plugin.s3;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Checks a request's AWS Signature Version 4 with the s3 plugin's verifier (package-private
 * there), so that the iceberg plugin's signer is checked by independent code that DuckDB's
 * signed requests already pass.
 */
public final class SigV4Check {

    private SigV4Check() {
    }

    /**
     * Verifies one request.
     *
     * @param request request recorded by {@link FakeS3}
     * @param secrets secret key by access key ID
     * @return access key ID of a valid signature, empty when it is not valid
     */
    public static Optional<String> verify(FakeS3.Request request, Map<String, String> secrets) {
        return S3SigV4.verify(request.method(), request.path(), null, request.headers()::get,
                secrets::get, Instant.now());
    }
}
