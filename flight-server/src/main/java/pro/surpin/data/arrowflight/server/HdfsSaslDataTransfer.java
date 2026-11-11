package pro.surpin.data.arrowflight.server;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * SASL data transfer of the hadoopfs extension (item 41): the build {@code 51b7220-afc6} and later
 * speak SASL data-transfer protection to DataNodes on the read path, so on a cluster with
 * {@code dfs.data.transfer.protection} DuckDB reads non-local blocks directly instead of only
 * short-circuit. The setting {@code hdfsSaslDataTransfer} ({@code arrowflight.hdfs.saslDataTransfer},
 * {@code HDFS_SASL_DATA_TRANSFER}) is {@code auto} (default: direct reads when the loaded
 * extension's version says it supports them), {@code on} or {@code off} (short-circuit only, files
 * planned to nodes holding them whole, node-local copies for the rest).
 */
final class HdfsSaslDataTransfer {

    /** Client setting the extension reads to turn on Kerberos (LIBHDFS3_CONF). */
    static final String AUTHENTICATION = "hadoop.security.authentication";

    /** Client setting that asks DataNodes to encrypt the data transferred (LIBHDFS3_CONF). */
    static final String ENCRYPT_DATA_TRANSFER = "dfs.encrypt.data.transfer";

    /** Client setting naming the cipher suites of encrypted data transfer (LIBHDFS3_CONF). */
    static final String CIPHER_SUITES = "dfs.encrypt.data.transfer.cipher.suites";

    /** First hadoopfs build number (the {@code -afcN} suffix) that speaks SASL data transfer. */
    static final int FIRST_SASL_BUILD = 6;

    /** Bytes of DuckDB's extension metadata footer (metadata fields and signature). */
    private static final int FOOTER_BYTES = 512;

    /** Offset of the extension version field in the footer. */
    private static final int VERSION_OFFSET = 128;

    /** Length of one footer field. */
    private static final int FIELD_BYTES = 32;

    /** Build suffix of our hadoopfs builds, e.g. {@code 51b7220-afc6}. */
    private static final Pattern BUILD = Pattern.compile("-afc(\\d+)$");

    /** Value of hdfsSaslDataTransfer. */
    enum Mode {
        /** Direct reads when the extension supports them. */
        AUTO,
        /** Direct reads; the operator vouches for the extension. */
        ON,
        /** Short-circuit only, as before item 41. */
        OFF
    }

    /**
     * Outcome for this node.
     *
     * @param mode configured mode
     * @param direct whether DuckDB reads non-local blocks over SASL data transfer
     * @param extensionVersion version of the hadoopfs extension file, "unknown" when unreadable
     * @param reason why direct reads are off, null when they are on
     */
    record Decision(Mode mode, boolean direct, String extensionVersion, String reason) {

        /**
         * Describes the decision for the start line {@code hdfs=saslDataTransfer}.
         *
         * @return key=value pairs
         */
        String describe() {
            return "mode=" + mode.name().toLowerCase(Locale.ROOT) + " direct=" + direct
                    + " extensionVersion=" + extensionVersion
                    + (reason == null ? "" : " reason=\"" + reason + "\"");
        }
    }

    /** Not instantiable. */
    private HdfsSaslDataTransfer() {
    }

    /**
     * Resolves {@code hdfsSaslDataTransfer} from a configuration source.
     *
     * @param props configuration source
     * @return mode, AUTO when unset
     * @throws IllegalArgumentException for a value other than auto, on or off
     */
    static Mode mode(ConfigSource props) {
        String value = props.lookup(
                java.util.List.of("hdfsSaslDataTransfer", "arrowflight.hdfs.saslDataTransfer"),
                java.util.List.of("HDFS_SASL_DATA_TRANSFER")).orElse(null);
        if (value == null || value.isBlank()) {
            return Mode.AUTO;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "auto" -> Mode.AUTO;
            case "on", "true" -> Mode.ON;
            case "off", "false" -> Mode.OFF;
            default -> throw new IllegalArgumentException(
                    "hdfsSaslDataTransfer must be auto, on or off: " + value);
        };
    }

    /**
     * Decides whether DuckDB reads non-local blocks over SASL data transfer on a cluster whose
     * DataNodes require data-transfer protection. The extension learns the protection only from
     * the LIBHDFS3_CONF file, with Kerberos authentication there too; without both it cannot
     * read over TCP, so direct reads stay off ({@code auto}) or the start stops ({@code on}).
     *
     * @param mode configured mode
     * @param extensionVersion version of the hadoopfs extension file, empty when unknown
     * @param libhdfs3 properties of the LIBHDFS3_CONF file
     * @return decision
     * @throws IllegalStateException when {@code on} cannot work with the LIBHDFS3_CONF file
     */
    static Decision decide(Mode mode, Optional<String> extensionVersion,
            Map<String, String> libhdfs3) {
        String version = extensionVersion.orElse("unknown");
        if (mode == Mode.OFF) {
            return new Decision(mode, false, version, "hdfsSaslDataTransfer=off");
        }
        String missing = missingClientSettings(libhdfs3);
        if (missing != null) {
            if (mode == Mode.ON) {
                throw new IllegalStateException("hdfsSaslDataTransfer=on, but the hadoopfs"
                        + " extension cannot speak SASL data transfer: " + missing);
            }
            return new Decision(mode, false, version, missing);
        }
        if (mode == Mode.ON) {
            return new Decision(mode, true, version, null);
        }
        if (!supportsSasl(version)) {
            return new Decision(mode, false, version, "hadoopfs build " + version
                    + " does not speak SASL data transfer (51b7220-afc" + FIRST_SASL_BUILD
                    + " and later do); set hdfsSaslDataTransfer=on for another build that does");
        }
        return new Decision(mode, true, version, null);
    }

    /**
     * Describes the data-transfer QOP and cipher the LIBHDFS3_CONF file asks the extension for,
     * for the start line (item 46 (3)): what DataNodes really negotiate is known only inside the
     * extension, which reports none yet ({@code negotiated=unknown}).
     *
     * @param libhdfs3 properties of the LIBHDFS3_CONF file
     * @return key=value pairs
     */
    static String requestedSecurity(Map<String, String> libhdfs3) {
        return "requestedQop=" + valueOrUnset(libhdfs3.get(HdfsShortCircuit.TRANSFER_PROTECTION))
                + " encryptDataTransfer=" + valueOrUnset(libhdfs3.get(ENCRYPT_DATA_TRANSFER))
                + " cipherSuites=" + valueOrUnset(libhdfs3.get(CIPHER_SUITES))
                + " negotiated=unknown";
    }

    /**
     * Returns a setting's trimmed value with spaces removed, or {@code unset}.
     *
     * @param value setting value, may be null
     * @return value for a key=value pair
     */
    private static String valueOrUnset(String value) {
        return value == null || value.isBlank() ? "unset" : value.trim().replace(" ", "");
    }

    /**
     * Names what the LIBHDFS3_CONF file lacks for the extension's SASL data transfer.
     *
     * @param libhdfs3 properties of the LIBHDFS3_CONF file
     * @return what is missing, null when nothing is
     */
    static String missingClientSettings(Map<String, String> libhdfs3) {
        String protection = libhdfs3.get(HdfsShortCircuit.TRANSFER_PROTECTION);
        if (protection == null || protection.isBlank()) {
            return HdfsShortCircuit.TRANSFER_PROTECTION + " is not set in the LIBHDFS3_CONF file";
        }
        String authentication = libhdfs3.get(AUTHENTICATION);
        if (authentication == null || !"kerberos".equalsIgnoreCase(authentication.trim())) {
            return AUTHENTICATION + "=kerberos is not set in the LIBHDFS3_CONF file";
        }
        return null;
    }

    /**
     * Tells whether a hadoopfs version is one of our builds that speak SASL data transfer.
     *
     * @param version extension version, e.g. {@code 51b7220-afc6}
     * @return true for build afc6 and later
     */
    static boolean supportsSasl(String version) {
        if (version == null) {
            return false;
        }
        Matcher m = BUILD.matcher(version.trim());
        if (!m.find()) {
            return false;
        }
        try {
            return Integer.parseInt(m.group(1)) >= FIRST_SASL_BUILD;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Reads the extension version from a DuckDB extension file's metadata footer (the 512 bytes
     * at its end: metadata fields of 32 bytes, then the signature).
     *
     * @param file extension file, or null
     * @return version, empty when the file is missing or has no readable footer
     */
    static Optional<String> extensionVersion(Path file) {
        if (file == null) {
            return Optional.empty();
        }
        try (RandomAccessFile in = new RandomAccessFile(file.toFile(), "r")) {
            long length = in.length();
            if (length < FOOTER_BYTES) {
                return Optional.empty();
            }
            byte[] field = new byte[FIELD_BYTES];
            in.seek(length - FOOTER_BYTES + VERSION_OFFSET);
            in.readFully(field);
            int end = 0;
            while (end < field.length && field[end] != 0) {
                end++;
            }
            String version = new String(field, 0, end, StandardCharsets.US_ASCII).trim();
            return version.isEmpty() || !version.chars().allMatch(c -> c > 32 && c < 127)
                    ? Optional.empty() : Optional.of(version);
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
