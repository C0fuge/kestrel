package pro.surpin.data.arrowflight.auth.ldap;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;

/**
 * Admits a Basic user name and password the directory accepts, answering with a signed session
 * bearer token, and later calls carrying that token.
 */
final class LdapFlightAuthenticator implements CallHeaderAuthenticator {
    private static final Logger LOGGER = LoggerFactory.getLogger(LdapFlightAuthenticator.class);
    private static final int MAX_BASIC_LENGTH = 8192;
    private final Directory directory;
    private final LdapSessionToken sessions;
    private final LdapGroupCache groups;

    /**
     * An accepted login.
     *
     * @param identity the caller's identity
     * @param groups the caller's directory groups, empty without a group search
     */
    record Login(String identity, Set<String> groups) {
        /**
         * Copies the groups.
         */
        Login {
            groups = Set.copyOf(groups);
        }
    }

    /** Checks a login name and password and looks up groups. */
    interface Directory {
        /**
         * Checks credentials.
         *
         * @param user login name as sent by the client
         * @param password password
         * @return the accepted login, or null when the credentials are refused
         * @throws LdapDirectory.DirectoryUnavailableException when the directory cannot answer
         */
        Login login(String user, String password) throws LdapDirectory.DirectoryUnavailableException;

        /**
         * Looks up the groups of an identity a session token carries.
         *
         * @param identity identity of an earlier login
         * @return group names
         * @throws LdapDirectory.DirectoryUnavailableException when the directory cannot tell the groups
         */
        default Set<String> groups(String identity) throws LdapDirectory.DirectoryUnavailableException {
            return Set.of();
        }
    }

    /**
     * Creates the authenticator.
     *
     * @param directory credential check
     * @param sessions session token codec
     * @param groups group cache, filled at logins
     */
    LdapFlightAuthenticator(Directory directory, LdapSessionToken sessions, LdapGroupCache groups) {
        this.directory = directory;
        this.sessions = sessions;
        this.groups = groups;
    }

    @Override
    public AuthResult authenticate(CallHeaders headers) {
        String header = headers.get(Auth2Constants.AUTHORIZATION_HEADER);
        if (header != null && header.startsWith(Auth2Constants.BEARER_PREFIX)) {
            try {
                String user = sessions.verify(header.substring(Auth2Constants.BEARER_PREFIX.length()).trim());
                requireGroups(user);
                return () -> user;
            } catch (IllegalArgumentException e) {
                throw unauthenticated("Invalid or expired LDAP session; log in again");
            }
        }
        if (header == null || !header.startsWith(Auth2Constants.BASIC_PREFIX)
                || header.length() > MAX_BASIC_LENGTH) {
            throw unauthenticated("LDAP user name and password required");
        }
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(
                    header.substring(Auth2Constants.BASIC_PREFIX.length()).trim()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw unauthenticated("Malformed Basic credential");
        }
        int colon = decoded.indexOf(':');
        if (colon < 1) {
            throw unauthenticated("Malformed Basic credential");
        }
        String login = decoded.substring(0, colon);
        Login accepted;
        try {
            accepted = directory.login(login, decoded.substring(colon + 1));
        } catch (LdapDirectory.DirectoryUnavailableException e) {
            LOGGER.warn("LDAP login of {} not checked: {}", printable(login), e.getMessage());
            throw CallStatus.UNAVAILABLE.withDescription("LDAP directory unavailable").toRuntimeException();
        }
        if (accepted == null) {
            LOGGER.info("LDAP login of {} refused", printable(login));
            throw unauthenticated("Invalid LDAP user name or password");
        }
        String identity = accepted.identity();
        groups.put(identity, accepted.groups());
        String token = sessions.issue(identity);
        return new AuthResult() {
            @Override
            public String getPeerIdentity() {
                return identity;
            }

            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoing) {
                outgoing.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + token);
            }
        };
    }

    /**
     * Returns the groups of a session's identity, so a call whose groups cannot be told fails here
     * as unavailable instead of being denied later by the policy.
     *
     * @param identity identity from a valid session token
     */
    private void requireGroups(String identity) {
        try {
            groups.groups(identity);
        } catch (LdapDirectory.DirectoryUnavailableException e) {
            LOGGER.warn("LDAP groups of {} not read: {}", printable(identity), e.getMessage());
            throw CallStatus.UNAVAILABLE.withDescription("LDAP group lookup failed").toRuntimeException();
        }
    }

    /**
     * Builds an authentication error without credential details.
     *
     * @param message client-facing reason
     * @return status exception
     */
    private static RuntimeException unauthenticated(String message) {
        return CallStatus.UNAUTHENTICATED.withDescription(message).toRuntimeException();
    }

    /**
     * Shortens a client-sent name for a log line and drops control characters.
     *
     * @param login login name
     * @return loggable text
     */
    private static String printable(String login) {
        String shortened = login.length() > 64 ? login.substring(0, 64) + "..." : login;
        return shortened.replaceAll("\\p{Cntrl}", "?");
    }
}
