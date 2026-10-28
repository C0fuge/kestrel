package pro.surpin.data.arrowflight.auth.ldap;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Public face of {@link LdapTestDirectory} for tests in the server's package. */
public final class LdapTestAccess implements AutoCloseable {
    private final LdapTestDirectory directory;

    /**
     * Wraps a started directory.
     *
     * @param directory started directory
     */
    private LdapTestAccess(LdapTestDirectory directory) {
        this.directory = directory;
    }

    /**
     * Starts a directory with a certificate for localhost.
     *
     * @param dir scratch directory
     * @return started directory
     * @throws Exception when it cannot start
     */
    public static LdapTestAccess start(Path dir) throws Exception {
        return new LdapTestAccess(new LdapTestDirectory(dir, "dns:localhost,ip:127.0.0.1", true));
    }

    /**
     * Template-mode settings over StartTLS.
     *
     * @return mutable settings
     */
    public Map<String, String> templateSettings() {
        return directory.templateSettings();
    }

    /**
     * Writes the certificate and key as PEM.
     *
     * @param dir target directory
     * @return certificate and key paths
     * @throws Exception on key store failure
     */
    public List<Path> pem(Path dir) throws Exception {
        return directory.pem(dir);
    }

    @Override
    public void close() {
        directory.close();
    }
}
