package pro.surpin.data.arrowflight.server;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * Keeps a Kerberos ticket cache file fresh for the hadoopfs extension (libhdfs3 reads only a ticket
 * cache): logs in from the configured keytab, writes the ticket-granting ticket as a MIT file cache
 * and logs in again before the ticket runs out. Off unless a principal and a keytab are set.
 */
final class HdfsTicketCache implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTicketCache.class);

    private static final String MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    /** The libhdfs3 setting that names the ticket cache the extension reads. */
    static final String LIBHDFS3_TICKET_CACHE = "hadoop.security.kerberos.ticket.cache.path";

    /** A ticket is replaced this long before it ends at the latest (ms). */
    static final long MIN_REMAINING_MILLIS = 60_000;

    /** A ticket is replaced once this part of its lifetime has passed. */
    static final double RENEW_AT = 0.8;

    /** Wait before another login after a failed one (ms). */
    static final long RETRY_MILLIS = 60_000;

    /** Shortest wait between two logins (ms), so a very short ticket cannot spin the KDC. */
    static final long MIN_DELAY_MILLIS = 10_000;

    /**
     * Resolved settings.
     *
     * @param principal client principal of the keytab
     * @param keytab keytab file
     * @param cache ticket cache file to write
     * @param extensionCache ticket cache file the extension reads, null when unknown
     */
    record Settings(String principal, String keytab, Path cache, Path extensionCache) {
    }

    private final Settings settings;
    private final ScheduledExecutorService renewer;
    private long ticketEnd;

    /**
     * Creates the cache writer; nothing is written before {@link #refresh()}.
     *
     * @param settings resolved settings
     */
    HdfsTicketCache(Settings settings) {
        this.settings = settings;
        this.renewer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "hdfs-ticket-cache");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Writes the ticket cache when a principal and a keytab are set and keeps it fresh.
     *
     * @param props configuration source
     * @param libhdfs3 properties of the LIBHDFS3_CONF file
     * @return the running writer, empty when not configured
     * @throws IllegalArgumentException when the settings are incomplete or invalid
     * @throws IllegalStateException when the first login or write fails
     */
    static Optional<HdfsTicketCache> start(ConfigSource props, Map<String, String> libhdfs3) {
        Optional<Settings> settings = settings(props, libhdfs3,
                props.environment("KRB5CCNAME").orElse(null), currentUid());
        if (settings.isEmpty()) {
            return Optional.empty();
        }
        HdfsTicketCache cache = new HdfsTicketCache(settings.get());
        Path reads = settings.get().extensionCache();
        if (!settings.get().cache().equals(reads)) {
            LOGGER.warn("HDFS ticket cache {} is not the one the hadoopfs extension reads ({}): set"
                    + " KRB5CCNAME or {} in the LIBHDFS3_CONF file to it.", settings.get().cache(),
                    reads == null ? "unknown" : reads, LIBHDFS3_TICKET_CACHE);
        }
        long delay;
        try {
            delay = cache.refresh();
        } catch (IOException e) {
            cache.close();
            throw new IllegalStateException(e.getMessage(), e);
        }
        cache.schedule(delay);
        return Optional.of(cache);
    }

    /**
     * Resolves the settings. Without principal and keytab the writer is off; the cache file
     * defaults to the one the extension reads: LIBHDFS3_CONF's
     * {@code hadoop.security.kerberos.ticket.cache.path}, else {@code KRB5CCNAME}, else
     * {@code /tmp/krb5cc_<uid>}.
     *
     * @param props configuration source
     * @param libhdfs3 properties of the LIBHDFS3_CONF file
     * @param krb5ccname the KRB5CCNAME environment value, or null
     * @param uid user id of the process, or -1 when unknown
     * @return settings, empty when no principal and keytab are set
     * @throws IllegalArgumentException when the settings are incomplete or name a cache that is
     *         not a file
     */
    static Optional<Settings> settings(ConfigSource props, Map<String, String> libhdfs3,
            String krb5ccname, long uid) {
        String principal = props.lookup(List.of("hdfsKerberosPrincipal",
                "arrowflight.hdfs.kerberos.principal"), List.of("HDFS_KERBEROS_PRINCIPAL")).orElse(null);
        String keytab = props.lookup(List.of("hdfsKerberosKeytab",
                "arrowflight.hdfs.kerberos.keytab"), List.of("HDFS_KERBEROS_KEYTAB")).orElse(null);
        String cache = props.lookup(List.of("hdfsKerberosTicketCache",
                "arrowflight.hdfs.kerberos.ticketCache"), List.of("HDFS_KERBEROS_TICKET_CACHE"))
                .orElse(null);
        if (principal == null && keytab == null) {
            if (cache != null) {
                throw new IllegalArgumentException("hdfsKerberosTicketCache needs hdfsKerberosPrincipal"
                        + " and hdfsKerberosKeytab");
            }
            return Optional.empty();
        }
        if (principal == null || keytab == null) {
            throw new IllegalArgumentException("hdfsKerberosPrincipal and hdfsKerberosKeytab must be"
                    + " set together");
        }
        Path extension = extensionCache(libhdfs3.get(LIBHDFS3_TICKET_CACHE), krb5ccname, uid);
        Path target = cache != null ? fileCache(cache, "hdfsKerberosTicketCache") : extension;
        if (target == null) {
            throw new IllegalArgumentException("Set hdfsKerberosTicketCache: the user id is unknown,"
                    + " so the default ticket cache cannot be named");
        }
        return Optional.of(new Settings(principal, keytab, target, extension));
    }

    /**
     * Names the ticket cache libhdfs3 reads.
     *
     * @param configured LIBHDFS3_CONF's ticket cache path, or null
     * @param krb5ccname the KRB5CCNAME environment value, or null
     * @param uid user id of the process, or -1 when unknown
     * @return the cache file, null when it cannot be named
     */
    static Path extensionCache(String configured, String krb5ccname, long uid) {
        if (configured != null && !configured.isBlank()) {
            return fileCache(configured, LIBHDFS3_TICKET_CACHE);
        }
        if (krb5ccname != null && !krb5ccname.isBlank()) {
            return fileCache(krb5ccname, "KRB5CCNAME");
        }
        return uid < 0 ? null : Path.of("/tmp/krb5cc_" + uid);
    }

    /**
     * Turns a cache name into a file path.
     *
     * @param name cache name, a path or {@code FILE:path}
     * @param setting name of the setting, for the error
     * @return absolute file path
     * @throws IllegalArgumentException when the name is another cache type (KEYRING:, DIR:, ...)
     */
    static Path fileCache(String name, String setting) {
        String value = name.trim();
        if (value.startsWith("FILE:")) {
            value = value.substring("FILE:".length());
        } else if (value.matches("[A-Za-z]+:.*")) {
            throw new IllegalArgumentException(setting + " must name a FILE: ticket cache: " + name);
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException(setting + " must name a FILE: ticket cache: " + name);
        }
        return Path.of(value).toAbsolutePath();
    }

    /**
     * Logs in from the keytab and replaces the cache file with the new ticket.
     *
     * @return wait before the next login (ms)
     * @throws IOException when the login or the write fails
     */
    long refresh() throws IOException {
        KerberosTicket tgt = login();
        write(settings.cache(), encode(tgt));
        long start = (tgt.getStartTime() == null ? tgt.getAuthTime() : tgt.getStartTime()).getTime();
        long end = tgt.getEndTime().getTime();
        synchronized (this) {
            ticketEnd = end;
        }
        LOGGER.info("hdfs=kerberosTicketCache principal={} path={} ticketEnd={}",
                tgt.getClient().getName(), settings.cache(), tgt.getEndTime().toInstant());
        return nextDelay(start, end, System.currentTimeMillis());
    }

    /**
     * Returns the wait before the next login.
     *
     * @param start ticket start (epoch ms)
     * @param end ticket end (epoch ms)
     * @param now current time (epoch ms)
     * @return ms until {@link #RENEW_AT} of the lifetime or {@link #MIN_REMAINING_MILLIS} before
     *         the end, whichever comes first, at least {@link #MIN_DELAY_MILLIS}
     */
    static long nextDelay(long start, long end, long now) {
        long at = Math.min(start + (long) ((end - start) * RENEW_AT), end - MIN_REMAINING_MILLIS);
        return Math.max(MIN_DELAY_MILLIS, at - now);
    }

    /**
     * Schedules the next login; a failed login keeps the old cache and is retried.
     *
     * @param delayMillis wait (ms)
     */
    private void schedule(long delayMillis) {
        if (renewer.isShutdown()) {
            return;
        }
        renewer.schedule(() -> {
            long next;
            try {
                next = refresh();
            } catch (IOException | RuntimeException e) {
                long end;
                synchronized (this) {
                    end = ticketEnd;
                }
                LOGGER.warn("HDFS ticket cache {} not renewed, retry in {} s (the cached ticket ends {}): {}",
                        settings.cache(), RETRY_MILLIS / 1000, new Date(end).toInstant(), e.getMessage());
                next = RETRY_MILLIS;
            }
            schedule(next);
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Logs in from the keytab.
     *
     * @return the ticket-granting ticket
     * @throws IOException when the login fails or yields no ticket-granting ticket
     */
    private KerberosTicket login() throws IOException {
        Subject subject = new Subject();
        try {
            new LoginContext("hdfs-ticket-cache", subject, null, configuration()).login();
        } catch (LoginException e) {
            throw new IOException("Kerberos login for the HDFS ticket cache failed (" + settings.principal()
                    + ", keytab " + settings.keytab() + "): " + e.getMessage(), e);
        }
        for (KerberosTicket ticket : subject.getPrivateCredentials(KerberosTicket.class)) {
            if (ticket.getServer().getName().startsWith("krbtgt/")) {
                return ticket;
            }
        }
        throw new IOException("Kerberos login for the HDFS ticket cache (" + settings.principal()
                + ") gave no ticket-granting ticket");
    }

    /**
     * Returns the JAAS configuration of a keytab login.
     *
     * @return configuration
     */
    private Configuration configuration() {
        Map<String, String> options = new HashMap<>();
        options.put("doNotPrompt", "true");
        options.put("isInitiator", "true");
        options.put("refreshKrb5Config", "true");
        options.put("useKeyTab", "true");
        options.put("keyTab", settings.keytab());
        options.put("storeKey", "false");
        options.put("principal", settings.principal());
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
     * Encodes a ticket as a MIT file credential cache, version 4, whose default principal is the
     * ticket's client.
     *
     * @param ticket the ticket
     * @return file content
     * @throws IOException never for a valid ticket
     */
    static byte[] encode(KerberosTicket ticket) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeShort(0x0504);
        out.writeShort(0);
        principal(out, ticket.getClient());
        principal(out, ticket.getClient());
        principal(out, ticket.getServer());
        out.writeShort(ticket.getSessionKeyType());
        counted(out, ticket.getSessionKey().getEncoded());
        Date auth = ticket.getAuthTime();
        Date start = ticket.getStartTime() == null ? auth : ticket.getStartTime();
        out.writeInt(seconds(auth));
        out.writeInt(seconds(start));
        out.writeInt(seconds(ticket.getEndTime()));
        out.writeInt(seconds(ticket.getRenewTill()));
        out.writeByte(0);
        out.writeInt(flags(ticket.getFlags()));
        InetAddress[] addresses = ticket.getClientAddresses();
        if (addresses == null) {
            out.writeInt(0);
        } else {
            out.writeInt(addresses.length);
            for (InetAddress address : addresses) {
                // RFC 4120 address types: 2 = IPv4, 24 = IPv6.
                out.writeShort(address instanceof Inet4Address ? 2 : 24);
                counted(out, address.getAddress());
            }
        }
        out.writeInt(0);
        counted(out, ticket.getEncoded());
        counted(out, new byte[0]);
        out.flush();
        return bytes.toByteArray();
    }

    /**
     * Packs ticket flags in RFC 4120 bit order (flag 0 is the highest bit).
     *
     * @param flags flags, may be null
     * @return packed flags
     */
    static int flags(boolean[] flags) {
        int packed = 0;
        if (flags != null) {
            for (int i = 0; i < Math.min(32, flags.length); i++) {
                if (flags[i]) {
                    packed |= 1 << (31 - i);
                }
            }
        }
        return packed;
    }

    /**
     * Converts a time to the cache's 32-bit seconds.
     *
     * @param time time, null for 0
     * @return seconds since the epoch
     */
    private static int seconds(Date time) {
        return time == null ? 0 : (int) (time.getTime() / 1000);
    }

    /**
     * Writes a principal: name type, component count, realm, components.
     *
     * @param out output
     * @param principal principal
     * @throws IOException never for an in-memory output
     */
    private static void principal(DataOutputStream out, KerberosPrincipal principal) throws IOException {
        List<String> components = components(principal.getName(), principal.getRealm());
        out.writeInt(principal.getNameType());
        out.writeInt(components.size());
        counted(out, principal.getRealm().getBytes(StandardCharsets.UTF_8));
        for (String component : components) {
            counted(out, component.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Splits a principal name into its components, undoing backslash escapes.
     *
     * @param name full name, {@code a/b@REALM}
     * @param realm the realm
     * @return components
     */
    static List<String> components(String name, String realm) {
        String suffix = "@" + realm;
        String body = realm != null && !realm.isEmpty() && name.endsWith(suffix)
                ? name.substring(0, name.length() - suffix.length()) : name;
        List<String> components = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '\\' && i + 1 < body.length()) {
                current.append(body.charAt(++i));
            } else if (c == '/') {
                components.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        components.add(current.toString());
        return components;
    }

    /**
     * Writes a 32-bit length and the bytes.
     *
     * @param out output
     * @param data bytes
     * @throws IOException never for an in-memory output
     */
    private static void counted(DataOutputStream out, byte[] data) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    /**
     * Replaces a file atomically with content only the owner can read.
     *
     * @param path target file
     * @param content content
     * @throws IOException when the file cannot be written
     */
    static void write(Path path, byte[] content) throws IOException {
        Path target = path.toAbsolutePath();
        Path dir = target.getParent();
        Path name = target.getFileName();
        if (dir == null || name == null) {
            throw new IOException("Not a file path: " + path);
        }
        // A temporary file is created owner-only (rw-------) on POSIX file systems.
        Path temp = Files.createTempFile(dir, "." + name + ".", ".tmp");
        try {
            Files.write(temp, content);
            try {
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    /**
     * Returns the user id of the process.
     *
     * @return user id, -1 when unknown (not a Unix system)
     */
    private static long currentUid() {
        try {
            return new com.sun.security.auth.module.UnixSystem().getUid();
        } catch (RuntimeException | LinkageError e) {
            return -1;
        }
    }

    /**
     * Stops renewing; the cache file stays until its ticket ends.
     */
    @Override
    public void close() {
        renewer.shutdownNow();
    }
}
