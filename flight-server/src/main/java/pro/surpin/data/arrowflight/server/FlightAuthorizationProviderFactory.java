package pro.surpin.data.arrowflight.server;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Creates a custom Flight authorization provider discovered with ServiceLoader. */
public interface FlightAuthorizationProviderFactory {
    /**
     * Returns the provider name used by FLIGHT_AUTHORIZATION_PROVIDER.
     *
     * @return unique provider name
     */
    String name();

    /**
     * Builds a provider from deployment settings.
     *
     * @param environment process environment
     * @return authorization provider
     */
    FlightAuthorization create(Map<String, String> environment);

    /**
     * Builds a provider that may also use the groups the authentication provider knows.
     *
     * @param environment process environment
     * @param providerGroups groups the authentication provider reports for an identity, empty when unknown
     * @return authorization provider
     */
    default FlightAuthorization create(Map<String, String> environment,
            Function<String, Set<String>> providerGroups) {
        return create(environment);
    }

    /**
     * Reports whether the policy needs a distinct authenticated caller identity.
     *
     * @return true when anonymous or shared identities are unsuitable
     */
    default boolean requiresIdentity() {
        return true;
    }
}
