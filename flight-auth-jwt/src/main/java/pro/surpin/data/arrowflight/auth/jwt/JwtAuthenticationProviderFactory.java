package pro.surpin.data.arrowflight.auth.jwt;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import pro.surpin.data.arrowflight.server.FlightAuthenticationProvider;
import pro.surpin.data.arrowflight.server.FlightAuthenticationProviderFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Set;

/**
 * Authentication provider {@code jwt} ({@code FLIGHT_AUTH_PROVIDER=jwt}): every call carries a JWT
 * from an OIDC provider or Knox as bearer token; its signature is checked against the issuer's key
 * set and its issuer, audience and expiry against the settings. The caller's identity is a claim of
 * the token (default {@code sub}), as a Kerberos principal is for the {@code kerberos} provider; with
 * {@code FLIGHT_JWT_GROUPS_CLAIM} the token's groups are added to the groups policy checks see.
 * Requires TLS on the Flight port.
 */
public final class JwtAuthenticationProviderFactory implements FlightAuthenticationProviderFactory {

    @Override
    public String name() {
        return "jwt";
    }

    @Override
    public FlightAuthenticationProvider create(Map<String, String> environment) {
        JwtSettings settings = JwtSettings.fromEnvironment(environment);
        JwtGroups groups = new JwtGroups(settings.groupsClaim() != null, Clock.systemUTC());
        JwtFlightAuthenticator authenticator = new JwtFlightAuthenticator(new JwtVerifier(settings), groups,
                settings.basicPassword());
        return new FlightAuthenticationProvider() {
            @Override
            public CallHeaderAuthenticator authenticator() {
                return authenticator;
            }

            @Override
            public Set<String> groups(String identity) {
                return groups.groups(identity);
            }

            @Override
            public boolean requiresTls() {
                return true;
            }

            @Override
            public boolean establishesIdentity() {
                return true;
            }

            @Override
            public String toString() {
                return "jwt " + settings;
            }
        };
    }
}
