package pro.surpin.data.arrowflight.auth.ldap;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import pro.surpin.data.arrowflight.server.FlightAuthenticationProvider;
import pro.surpin.data.arrowflight.server.FlightAuthenticationProviderFactory;

import java.time.Clock;
import java.util.Map;

/**
 * Authentication provider {@code ldap} ({@code FLIGHT_AUTH_PROVIDER=ldap}): a Basic user name and
 * password is checked with an LDAP or Active Directory bind; the caller's identity is the login name
 * (or an attribute of the user's entry), as a Kerberos principal is for the {@code kerberos} provider.
 * Requires TLS on the Flight port.
 */
public final class LdapAuthenticationProviderFactory implements FlightAuthenticationProviderFactory {

    @Override
    public String name() {
        return "ldap";
    }

    @Override
    public FlightAuthenticationProvider create(Map<String, String> environment) {
        LdapSettings settings = LdapSettings.fromEnvironment(environment);
        CallHeaderAuthenticator authenticator = new LdapFlightAuthenticator(new LdapDirectory(settings),
                new LdapSessionToken(settings.sessionSecret(), settings.sessionMillis(), Clock.systemUTC()));
        return new FlightAuthenticationProvider() {
            @Override
            public CallHeaderAuthenticator authenticator() {
                return authenticator;
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
                return "ldap " + settings;
            }
        };
    }
}
