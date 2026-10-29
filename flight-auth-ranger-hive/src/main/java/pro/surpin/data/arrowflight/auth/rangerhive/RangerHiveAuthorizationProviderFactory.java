package pro.surpin.data.arrowflight.auth.rangerhive;

import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.FlightAuthorizationProviderFactory;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Authorization provider {@code ranger-hive} ({@code FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive}): the Hive catalog's
 * tables are checked against the cluster's Hive Ranger service; a table with a mask, a row filter or a column
 * denial for the caller is refused.
 */
public final class RangerHiveAuthorizationProviderFactory implements FlightAuthorizationProviderFactory {

    @Override
    public String name() {
        return "ranger-hive";
    }

    @Override
    public FlightAuthorization create(Map<String, String> environment) {
        return create(environment, identity -> Set.of());
    }

    @Override
    public FlightAuthorization create(Map<String, String> environment,
            Function<String, Set<String>> providerGroups) {
        return RangerHiveAuthorization.fromSettings(environment, providerGroups);
    }
}
