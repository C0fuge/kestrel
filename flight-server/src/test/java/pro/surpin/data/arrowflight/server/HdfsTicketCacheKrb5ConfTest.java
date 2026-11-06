package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * Tests that the keytab writer finds the ticket cache the hadoopfs extension reads through the krb5
 * configuration's default_ccache_name, and refuses to start when that cache is not a file (STATUS
 * item 36 (2): a keytab-only start on a host whose krb5.conf names a keyring answered every query
 * with "Table ... does not exist").
 */
@Tag("unit")
class HdfsTicketCacheKrb5ConfTest {

    private static final Map<String, String> LOGIN = Map.of("HDFS_KERBEROS_PRINCIPAL", "flight@CI.TEST",
            "HDFS_KERBEROS_KEYTAB", "/k");

    @TempDir
    Path dir;

    @Test
    void theDefaultCacheNameIsReadFromLibdefaultsAndIncludes() throws IOException {
        Path d = Files.createDirectories(dir.resolve("krb5.conf.d"));
        write(d.resolve("kcm_default_ccache"), "[libdefaults]\n    default_ccache_name = KCM:\n");
        write(d.resolve("ignored.rpmsave"), "[libdefaults]\n default_ccache_name = FILE:/wrong\n");
        Path main = write(dir.resolve("krb5.conf"), "includedir " + d + "\n[realms]\n"
                + " default_ccache_name = FILE:/not-libdefaults\n[libdefaults]\n dns_lookup_kdc = false\n");
        assertEquals("KCM:", HdfsTicketCache.krb5DefaultCacheName(main.toString()));

        Path keyring = write(dir.resolve("keyring.conf"),
                "# comment\n[libdefaults]\n default_ccache_name = KEYRING:persistent:%{uid}\n");
        Path other = write(dir.resolve("other.conf"), "[libdefaults]\n default_ccache_name = FILE:/x\n");
        assertEquals("KEYRING:persistent:%{uid}",
                HdfsTicketCache.krb5DefaultCacheName(keyring + ":" + other));
        Path included = write(dir.resolve("inc.conf"), "include " + other + "\n");
        assertEquals("FILE:/x", HdfsTicketCache.krb5DefaultCacheName(included.toString()));

        Path none = write(dir.resolve("none.conf"), "[libdefaults]\n default_realm = CI.TEST\n");
        assertNull(HdfsTicketCache.krb5DefaultCacheName(none.toString()));
        assertNull(HdfsTicketCache.krb5DefaultCacheName(dir.resolve("missing.conf").toString()));
    }

    @Test
    void userParametersAreExpanded() {
        assertEquals("/tmp/krb5cc_1000", HdfsTicketCache.expandCacheName("FILE:%{TEMP}/krb5cc_%{uid}", 1000)
                .substring("FILE:".length()));
        assertEquals("KEYRING:persistent:7", HdfsTicketCache.expandCacheName("KEYRING:persistent:%{uid}", 7));
        assertNull(HdfsTicketCache.expandCacheName("FILE:/tmp/krb5cc_%{uid}", -1));
        assertNull(HdfsTicketCache.expandCacheName("FILE:%{LIBDIR}/cc", 7));
        assertNull(HdfsTicketCache.expandCacheName(null, 7));
    }

    @Test
    void aFileDefaultCacheIsWhereTheLoginIsWritten() {
        HdfsTicketCache.Settings s = HdfsTicketCache.settings(source(), Map.of(), null,
                "FILE:/run/user/%{uid}/krb5cc", 1000).orElseThrow();
        assertEquals(Path.of("/run/user/1000/krb5cc"), s.cache());
        assertEquals(s.cache(), s.extensionCache());
        // KRB5CCNAME and LIBHDFS3_CONF still come first; an unknown parameter keeps the old default.
        assertEquals(Path.of("/tmp/env"), HdfsTicketCache.settings(source(), Map.of(), "/tmp/env",
                "KEYRING:persistent:%{uid}", 1000).orElseThrow().cache());
        assertEquals(Path.of("/tmp/krb5cc_1000"), HdfsTicketCache.settings(source(), Map.of(), null,
                "FILE:%{LIBDIR}/cc", 1000).orElseThrow().cache());
    }

    @Test
    void aKeyringDefaultCacheFailsTheStartWithTheCause() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> HdfsTicketCache.settings(
                source(), Map.of(), null, "KEYRING:persistent:%{uid}", 1000));
        assertTrue(e.getMessage().contains("KEYRING:persistent:%{uid}"), e.getMessage());
        assertTrue(e.getMessage().contains("KRB5CCNAME=FILE:"), e.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> HdfsTicketCache.settings(source(), Map.of(), null, "KCM:", 1000));
        // Without a keytab nothing is written, so nothing is checked.
        assertTrue(HdfsTicketCache.settings(new ConfigSource(name -> null, new Properties(), Map.of(), null),
                Map.of(), null, "KCM:", 1000).isEmpty());
    }

    private static ConfigSource source() {
        return new ConfigSource(name -> null, new Properties(), LOGIN, null);
    }

    private static Path write(Path file, String text) throws IOException {
        Files.writeString(file, text, StandardCharsets.UTF_8);
        return file;
    }
}
