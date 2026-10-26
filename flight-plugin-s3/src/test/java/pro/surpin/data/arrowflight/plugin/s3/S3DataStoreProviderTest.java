package pro.surpin.data.arrowflight.plugin.s3;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the startup check of {@link S3DataStoreProvider} on the host's port settings.
 */
@Tag("unit")
class S3DataStoreProviderTest {

    /** Verifies the warning names the setting, the port count and the reads it allows (item 0g). */
    @Test
    void warnsWhenPortsInTimeWaitAreNotReused() {
        Optional<String> warning = S3DataStoreProvider.ephemeralPortWarning("2", "32768\t60999\n");

        assertTrue(warning.isPresent());
        assertTrue(warning.get().startsWith("net.ipv4.tcp_tw_reuse=2:"), warning.get());
        assertTrue(warning.get().contains("28232 local ports (ip_local_port_range 32768-60999)"),
                warning.get());
        assertTrue(warning.get().contains("about 470 per second"), warning.get());
        assertTrue(warning.get().contains("net.ipv4.tcp_tw_reuse=1"), warning.get());
        assertTrue(S3DataStoreProvider.ephemeralPortWarning("0", null).isPresent());
        assertTrue(S3DataStoreProvider.ephemeralPortWarning("0", "odd").get()
                .contains("the local port range bound"));
    }

    /** Verifies nothing is said when ports are reused or the settings cannot be read. */
    @Test
    void silentWhenPortsAreReusedOrUnknown() {
        assertEquals(Optional.empty(), S3DataStoreProvider.ephemeralPortWarning("1\n", "1024 65535"));
        assertEquals(Optional.empty(), S3DataStoreProvider.ephemeralPortWarning(null, "1024 65535"));
    }
}
