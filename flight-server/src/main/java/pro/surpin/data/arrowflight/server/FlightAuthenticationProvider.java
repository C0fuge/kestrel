package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Set;

/** Supplies a Flight request authenticator and its startup requirements. */
public interface FlightAuthenticationProvider extends AutoCloseable {
    /**
     * Prepares credentials before Hadoop services are constructed.
     *
     * @param hadoopConfig shared Hadoop configuration
     * @throws IOException if credentials cannot be initialized
     */
    default void initialize(Configuration hadoopConfig) throws IOException {
    }

    /**
     * Returns the request authenticator.
     *
     * @return authenticator, or null for explicitly insecure development mode
     * @throws IOException if credentials cannot be loaded
     */
    CallHeaderAuthenticator authenticator() throws IOException;

    /**
     * Reports whether this provider needs TLS to protect credentials.
     *
     * @return true when TLS is required
     */
    boolean requiresTls();

    /**
     * Reports whether the provider supplies a distinct authenticated caller identity.
     *
     * @return true when policy engines can identify individual callers
     */
    boolean establishesIdentity();

    /**
     * Returns the groups the provider itself knows for an identity it authenticated, for policy
     * checks; they are added to the groups of the Hadoop group mapping.
     *
     * @param identity peer identity this provider established
     * @return provider groups, empty when the provider has none
     * @throws RuntimeException when the groups cannot be determined; policy checks then deny
     */
    default Set<String> groups(String identity) {
        return Set.of();
    }

    @Override
    default void close() {
    }
}
