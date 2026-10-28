package pro.surpin.data.arrowflight.auth.ldap;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import pro.surpin.data.arrowflight.server.FlightAuthenticationProvider;
import pro.surpin.data.arrowflight.server.FlightAuthenticationProviderFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Set;

/**
 * Authentication provider {@code ldap} ({@code FLIGHT_AUTH_PROVIDER=ldap}): a Basic user name and
 * password is checked with an LDAP or Active Directory bind; the caller's identity is the login name
 * (or an attribute of the user's entry), as a Kerberos principal is for the {@code kerberos} provider.
 * With {@code FLIGHT_LDAP_GROUP_SEARCH_BASE} the user's directory groups are added to the groups
 * policy checks see. Requires TLS on the Flight port.
 */
public final class LdapAuthenticationProviderFactory implements FlightAuthenticationProviderFactory {

    @Override
    public String name() {
        return "ldap";
    }

    @Override
    public FlightAuthenticationProvider create(Map<String, String> environment) {
        LdapSettings settings = LdapSettings.fromEnvironment(environment);
        LdapDirectory directory = new LdapDirectory(settings);
        LdapGroupCache groups = new LdapGroupCache(directory, settings.groupSearchBase() != null,
                settings.sessionMillis(), Clock.systemUTC());
        LdapFlightAuthenticator authenticator = new LdapFlightAuthenticator(directory,
                new LdapSessionToken(settings.sessionSecret(), settings.sessionMillis(), Clock.systemUTC()),
                groups);
        return new FlightAuthenticationProvider() {
            @Override
            public CallHeaderAuthenticator authenticator() {
                return authenticator;
            }

            @Override
            public Set<String> groups(String identity) {
                try {
                    return groups.groups(identity);
                } catch (LdapDirectory.DirectoryUnavailableException e) {
                    throw new IllegalStateException("LDAP groups unknown: " + e.getMessage(), e);
                }
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
