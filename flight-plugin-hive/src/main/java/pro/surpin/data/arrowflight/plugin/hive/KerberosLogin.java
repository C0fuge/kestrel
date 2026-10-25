package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Kerberos login of the metastore client: from a keytab (principal and keytab set) or from
 * the ticket cache ({@code KRB5CCNAME} or the default cache, kept fresh by an external
 * {@code kinit}). The login is renewed before its ticket-granting ticket runs out.
 */
final class KerberosLogin {

    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosLogin.class);

    private static final String MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    /** A login is renewed this long before its ticket ends at the latest (ms). */
    static final long MIN_REMAINING_MILLIS = 60_000;

    /** A login is renewed once this part of its ticket's lifetime has passed. */
    static final double RENEW_AT = 0.8;

    private final String principal;
    private final String keytab;
    private final String ticketCache;
    private Subject subject;
    private long ticketStart;
    private long ticketEnd;

    /**
     * Creates the login; logs in on first use.
     *
     * @param principal client principal, null to take the one of the ticket cache
     * @param keytab keytab file, null for the ticket cache
     * @param ticketCache ticket cache file, null for {@code KRB5CCNAME} or the default cache
     */
    KerberosLogin(String principal, String keytab, String ticketCache) {
        this.principal = principal;
        this.keytab = keytab;
        this.ticketCache = ticketCache;
    }

    /**
     * Returns the logged-in subject, logging in again when the ticket is about to run out.
     *
     * @return subject with a ticket-granting ticket
     * @throws IOException when the login fails
     */
    synchronized Subject subject() throws IOException {
        if (subject == null || needsRenewal(ticketStart, ticketEnd, System.currentTimeMillis())) {
            login();
        }
        return subject;
    }

    /**
     * Tells whether a login must be renewed.
     *
     * @param start ticket start (epoch ms)
     * @param end ticket end (epoch ms)
     * @param now current time (epoch ms)
     * @return true when the ticket ends within {@link #MIN_REMAINING_MILLIS} or {@link #RENEW_AT}
     *         of its lifetime has passed
     */
    static boolean needsRenewal(long start, long end, long now) {
        return now >= end - MIN_REMAINING_MILLIS || now >= start + (long) ((end - start) * RENEW_AT);
    }

    /**
     * Logs in and remembers the ticket's lifetime.
     *
     * @throws IOException when the login fails or yields no ticket-granting ticket
     */
    private void login() throws IOException {
        Subject fresh = new Subject();
        String source = keytab != null ? "keytab " + keytab
                : ticketCache != null ? "ticket cache " + ticketCache : "ticket cache";
        try {
            new LoginContext("hive-metastore", fresh, null, configuration()).login();
        } catch (LoginException e) {
            throw new IOException("Kerberos login for the Hive Metastore failed ("
                    + (principal == null ? "" : principal + ", ") + source + "): " + e.getMessage(), e);
        }
        KerberosTicket tgt = null;
        for (KerberosTicket ticket : fresh.getPrivateCredentials(KerberosTicket.class)) {
            KerberosPrincipal server = ticket.getServer();
            if (server.getName().startsWith("krbtgt/")) {
                tgt = ticket;
            }
        }
        if (tgt == null) {
            throw new IOException("Kerberos login for the Hive Metastore (" + source
                    + ") gave no ticket-granting ticket");
        }
        subject = fresh;
        ticketStart = tgt.getStartTime() == null ? tgt.getAuthTime().getTime() : tgt.getStartTime().getTime();
        ticketEnd = tgt.getEndTime().getTime();
        LOGGER.info("hive=kerberosLogin principal={} source={} ticketEnd={}", tgt.getClient().getName(),
                keytab == null ? "ticketCache" : "keytab", tgt.getEndTime().toInstant());
    }

    /**
     * Returns the JAAS configuration of the Kerberos login module.
     *
     * @return configuration
     */
    private Configuration configuration() {
        Map<String, String> options = new HashMap<>();
        options.put("doNotPrompt", "true");
        options.put("isInitiator", "true");
        options.put("refreshKrb5Config", "true");
        if (keytab != null) {
            options.put("useKeyTab", "true");
            options.put("keyTab", keytab);
            options.put("storeKey", "false");
        } else {
            options.put("useTicketCache", "true");
            if (ticketCache != null) {
                options.put("ticketCache", ticketCache);
            }
        }
        if (principal != null) {
            options.put("principal", principal);
        }
        AppConfigurationEntry entry = new AppConfigurationEntry(MODULE,
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {entry};
            }
        };
    }

    /**
     * Returns the metastore's service principal for one host: {@code _HOST} becomes the host of
     * the metastore URI in lower case, as the Hive client does.
     *
     * @param principal configured principal, {@code service/host@REALM}
     * @param host metastore host
     * @return service name and host
     */
    static String[] servicePrincipal(String principal, String host) {
        String[] parts = checkedPrincipal(principal);
        String instance = "_HOST".equals(parts[1]) ? host.toLowerCase(Locale.ROOT) : parts[1];
        return new String[] {parts[0], instance};
    }

    /**
     * Splits a service principal.
     *
     * @param principal {@code service/host@REALM}
     * @return service, host and realm
     * @throws IllegalArgumentException when it has not that form
     */
    static String[] checkedPrincipal(String principal) {
        String[] parts = principal.split("[/@]", -1);
        if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty() || parts[2].isEmpty()) {
            throw new IllegalArgumentException("hive.metastorePrincipal must be service/host@REALM"
                    + " (for example hive/_HOST@EXAMPLE.COM): " + principal);
        }
        return parts;
    }
}
