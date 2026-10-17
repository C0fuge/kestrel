package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import pro.surpin.data.arrowflight.server.FlightAuthenticationProvider;
import pro.surpin.data.arrowflight.server.FlightAuthenticationProviderFactory;

import java.util.Map;

/**
 * Authentication provider {@code token}: one shared access token, accepted as a bearer token or
 * as a Basic password (JDBC and StarRocks log in with a password). Select it with
 * {@code FLIGHT_AUTH_PROVIDER=token}; the token comes from {@code FLIGHT_AUTH_TOKEN} or the
 * {@code authToken} setting. Unlike the built-in {@code bearer} provider it does not insist on
 * TLS; put the server behind TLS ({@code FLIGHT_TLS_CERT}, {@code FLIGHT_TLS_KEY}) outside a lab.
 */
public final class TokenAuthenticationProviderFactory implements FlightAuthenticationProviderFactory {

    @Override
    public String name() {
        return "token";
    }

    @Override
    public FlightAuthenticationProvider create(Map<String, String> environment) {
        String token = environment.get("FLIGHT_AUTH_TOKEN");
        if (token == null || token.isBlank()) {
            token = ConfigAdapter.getConfig().authToken();
        }
        CallHeaderAuthenticator authenticator = new TokenAuthenticator(token);
        return new FlightAuthenticationProvider() {
            @Override
            public CallHeaderAuthenticator authenticator() {
                return authenticator;
            }

            @Override
            public boolean requiresTls() {
                return false;
            }

            @Override
            public boolean establishesIdentity() {
                return false;
            }
        };
    }
}
