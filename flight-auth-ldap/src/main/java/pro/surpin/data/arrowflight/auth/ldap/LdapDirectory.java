package pro.surpin.data.arrowflight.auth.ldap;

import com.unboundid.ldap.sdk.ExtendedResult;
import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.extensions.StartTLSExtendedRequest;
import com.unboundid.util.ssl.HostNameSSLSocketVerifier;
import com.unboundid.util.ssl.JVMDefaultTrustManager;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustStoreTrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Checks user names and passwords with simple binds against the configured directory servers and
 * reads the users' groups.
 */
final class LdapDirectory implements LdapFlightAuthenticator.Directory {
    private static final Logger LOGGER = LoggerFactory.getLogger(LdapDirectory.class);
    /** Login names: letters, digits and {@code . _ - @}, so neither DNs nor filters need escaping. */
    private static final Pattern USER = Pattern.compile("[\\p{L}\\p{N}._@-]{1,256}");
    /** Most groups one user may have; more fail the lookup rather than drop some. */
    static final int MAX_GROUPS = 1000;
    private final LdapSettings settings;
    private final SSLUtil ssl;
    private final LDAPConnectionOptions options;

    /**
     * Creates a directory client.
     *
     * @param settings validated provider settings
     */
    LdapDirectory(LdapSettings settings) {
        this.settings = settings;
        this.ssl = settings.trustStore() == null
                ? new SSLUtil(JVMDefaultTrustManager.getInstance())
                : new SSLUtil(new TrustStoreTrustManager(settings.trustStore().toFile(),
                        settings.trustStorePassword() == null ? null
                                : settings.trustStorePassword().toCharArray(),
                        settings.trustStoreType(), true));
        this.options = new LDAPConnectionOptions();
        options.setConnectTimeoutMillis(settings.connectTimeoutMillis());
        options.setResponseTimeoutMillis(settings.responseTimeoutMillis());
        options.setSSLSocketVerifier(new HostNameSSLSocketVerifier(true));
        options.setBindWithDNRequiresPassword(true);
    }

    /**
     * Reports whether a login name is acceptable.
     *
     * @param user login name
     * @return true when it uses only the allowed characters
     */
    static boolean acceptableUser(String user) {
        return user != null && USER.matcher(user).matches();
    }

    /**
     * Checks credentials and returns only the identity.
     *
     * @param user login name
     * @param password password
     * @return identity, or null when refused
     * @throws DirectoryUnavailableException when the directory cannot answer
     */
    String authenticate(String user, String password) throws DirectoryUnavailableException {
        LdapFlightAuthenticator.Login login = login(user, password);
        return login == null ? null : login.identity();
    }

    @Override
    public LdapFlightAuthenticator.Login login(String user, String password) throws DirectoryUnavailableException {
        if (!acceptableUser(user) || password == null || password.isEmpty()) {
            return null;
        }
        try (LDAPConnection connection = connect()) {
            if (settings.searchBase() == null) {
                String bindName = settings.userDnTemplate().replace("{0}", user);
                return bind(connection, bindName, password) ? accepted(connection, user, bindName) : null;
            }
            SearchResultEntry entry = findUser(connection, user);
            if (entry == null || !bind(connection, entry.getDN(), password)) {
                return null;
            }
            if (settings.userAttribute() == null) {
                return accepted(connection, user, entry.getDN());
            }
            String[] values = entry.getAttributeValues(settings.userAttribute());
            if (values == null || values.length != 1 || values[0].isBlank() || values[0].length() > 1024
                    || values[0].indexOf('\n') >= 0) {
                LOGGER.warn("LDAP login of {} refused: entry has no single usable {} value",
                        user, settings.userAttribute());
                return null;
            }
            return accepted(connection, values[0], entry.getDN());
        } catch (LDAPException e) {
            if (unavailable(e.getResultCode())) {
                throw new DirectoryUnavailableException("LDAP directory unavailable: " + e.getResultCode(), e);
            }
            LOGGER.warn("LDAP login of {} refused: {}", user, e.getResultCode());
            return null;
        }
    }

    @Override
    public Set<String> groups(String identity) throws DirectoryUnavailableException {
        if (settings.groupSearchBase() == null) {
            return Set.of();
        }
        try (LDAPConnection connection = connect()) {
            bindService(connection);
            String userDn;
            if (settings.searchBase() == null) {
                if (!acceptableUser(identity)) {
                    throw new DirectoryUnavailableException("LDAP identity unusable in a DN", null);
                }
                userDn = settings.userDnTemplate().replace("{0}", identity);
            } else {
                String encoded = Filter.encodeValue(identity);
                Filter filter = Filter.create(settings.userAttribute() == null
                        ? settings.searchFilter().replace("{0}", encoded)
                        : "(" + settings.userAttribute() + "=" + encoded + ")");
                SearchRequest request = new SearchRequest(settings.searchBase(), SearchScope.SUB, filter, "1.1");
                request.setSizeLimit(2);
                SearchResult result = connection.search(request);
                if (result.getEntryCount() != 1) {
                    throw new DirectoryUnavailableException("LDAP finds " + result.getEntryCount()
                            + " entries for the identity", null);
                }
                userDn = result.getSearchEntries().get(0).getDN();
            }
            return searchGroups(connection, identity, userDn);
        } catch (LDAPException e) {
            throw new DirectoryUnavailableException("LDAP group lookup failed: " + e.getResultCode(), e);
        }
    }

    /**
     * Builds an accepted login, reading the groups when a group search is configured.
     *
     * @param connection connection bound as the user
     * @param identity caller's identity
     * @param userDn user's DN
     * @return accepted login
     * @throws DirectoryUnavailableException when the groups cannot be read
     */
    private LdapFlightAuthenticator.Login accepted(LDAPConnection connection, String identity, String userDn)
            throws DirectoryUnavailableException {
        if (settings.groupSearchBase() == null) {
            return new LdapFlightAuthenticator.Login(identity, Set.of());
        }
        try {
            bindService(connection);
            return new LdapFlightAuthenticator.Login(identity, searchGroups(connection, identity, userDn));
        } catch (LDAPException e) {
            throw new DirectoryUnavailableException("LDAP group lookup failed: " + e.getResultCode(), e);
        }
    }

    /**
     * Binds as the service account when one is configured.
     *
     * @param connection open connection
     * @throws LDAPException on directory failure
     * @throws DirectoryUnavailableException when the directory refuses the service account
     */
    private void bindService(LDAPConnection connection) throws LDAPException, DirectoryUnavailableException {
        if (settings.bindDn() != null && !bind(connection, settings.bindDn(), settings.bindPassword())) {
            throw new DirectoryUnavailableException("LDAP directory refuses FLIGHT_LDAP_BIND_DN", null);
        }
    }

    /**
     * Reads the names of the groups a user belongs to.
     *
     * @param connection bound connection
     * @param identity caller's identity, for {@code {0}}
     * @param userDn user's DN, for {@code {1}}
     * @return group names
     * @throws LDAPException on directory failure, or more than {@link #MAX_GROUPS} groups
     */
    private Set<String> searchGroups(LDAPConnection connection, String identity, String userDn)
            throws LDAPException {
        Filter filter = Filter.create(settings.groupSearchFilter()
                .replace("{0}", Filter.encodeValue(identity)).replace("{1}", Filter.encodeValue(userDn)));
        SearchRequest request = new SearchRequest(settings.groupSearchBase(), SearchScope.SUB, filter,
                settings.groupNameAttribute());
        request.setSizeLimit(MAX_GROUPS);
        Set<String> groups = new HashSet<>();
        for (SearchResultEntry entry : connection.search(request).getSearchEntries()) {
            String name = entry.getAttributeValue(settings.groupNameAttribute());
            if (name != null && !name.isBlank()) {
                groups.add(name);
            }
        }
        return Set.copyOf(groups);
    }

    /**
     * Binds a connection as a user.
     *
     * @param connection open connection
     * @param bindName DN or bind name
     * @param password password
     * @return true on success, false when the directory rejects the credentials
     * @throws LDAPException on any other failure
     */
    private static boolean bind(LDAPConnection connection, String bindName, String password)
            throws LDAPException {
        try {
            connection.bind(new SimpleBindRequest(bindName, password));
            return true;
        } catch (LDAPException e) {
            if (e.getResultCode() == ResultCode.INVALID_CREDENTIALS) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Finds the one entry of a login name.
     *
     * @param connection open connection
     * @param user login name
     * @return entry, or null when none or several match
     * @throws LDAPException on directory failure
     * @throws DirectoryUnavailableException when the directory refuses the service account
     */
    private SearchResultEntry findUser(LDAPConnection connection, String user)
            throws LDAPException, DirectoryUnavailableException {
        bindService(connection);
        Filter filter = Filter.create(settings.searchFilter().replace("{0}", Filter.encodeValue(user)));
        SearchRequest request = settings.userAttribute() == null
                ? new SearchRequest(settings.searchBase(), SearchScope.SUB, filter, "1.1")
                : new SearchRequest(settings.searchBase(), SearchScope.SUB, filter, settings.userAttribute());
        request.setSizeLimit(2);
        SearchResult result;
        try {
            result = connection.search(request);
        } catch (LDAPException e) {
            if (e.getResultCode() == ResultCode.SIZE_LIMIT_EXCEEDED) {
                LOGGER.warn("LDAP login of {} refused: several entries match", user);
                return null;
            }
            throw e;
        }
        if (result.getEntryCount() != 1) {
            LOGGER.info("LDAP login of {} refused: {} entries match", user, result.getEntryCount());
            return null;
        }
        return result.getSearchEntries().get(0);
    }

    /**
     * Opens a connection to the first reachable server, with TLS as configured.
     *
     * @return open connection
     * @throws LDAPException when no server can be reached or TLS fails
     */
    private LDAPConnection connect() throws LDAPException {
        LDAPException last = null;
        for (LdapSettings.Server server : settings.servers()) {
            try {
                return connect(server);
            } catch (LDAPException e) {
                LOGGER.warn("LDAP server {}:{} failed: {}", server.host(), server.port(), e.getResultCode());
                last = e;
            }
        }
        throw new LDAPException(ResultCode.CONNECT_ERROR, "No LDAP server reachable", last);
    }

    /**
     * Opens one connection.
     *
     * @param server server address
     * @return open connection
     * @throws LDAPException when the connection or TLS fails
     */
    private LDAPConnection connect(LdapSettings.Server server) throws LDAPException {
        try {
            if (server.ssl()) {
                return new LDAPConnection(ssl.createSSLSocketFactory(), options, server.host(), server.port());
            }
            LDAPConnection connection = new LDAPConnection(options, server.host(), server.port());
            if (settings.startTls()) {
                try {
                    ExtendedResult result = connection.processExtendedOperation(
                            new StartTLSExtendedRequest(ssl.createSSLSocketFactory()));
                    if (result.getResultCode() != ResultCode.SUCCESS) {
                        throw new LDAPException(ResultCode.CONNECT_ERROR,
                                "StartTLS refused: " + result.getResultCode());
                    }
                } catch (LDAPException e) {
                    connection.close();
                    throw new LDAPException(ResultCode.CONNECT_ERROR, "StartTLS failed", e);
                }
            }
            return connection;
        } catch (GeneralSecurityException e) {
            throw new LDAPException(ResultCode.CONNECT_ERROR, "TLS setup failed", e);
        }
    }

    /**
     * Tells directory outages apart from refused credentials.
     *
     * @param code LDAP result code
     * @return true when the directory could not answer
     */
    private static boolean unavailable(ResultCode code) {
        return code == ResultCode.CONNECT_ERROR || code == ResultCode.SERVER_DOWN
                || code == ResultCode.TIMEOUT || code == ResultCode.UNAVAILABLE || code == ResultCode.BUSY
                || code == ResultCode.LOCAL_ERROR || code == ResultCode.DECODING_ERROR
                || code == ResultCode.ENCODING_ERROR;
    }

    /** The directory could not be asked: no server reachable, TLS failure, timeout. */
    static final class DirectoryUnavailableException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param message reason
         * @param cause directory failure
         */
        DirectoryUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
