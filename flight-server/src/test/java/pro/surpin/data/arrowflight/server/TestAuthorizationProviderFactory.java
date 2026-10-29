package pro.surpin.data.arrowflight.server;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Test policy provider registered through Java ServiceLoader. */
public final class TestAuthorizationProviderFactory implements FlightAuthorizationProviderFactory {
    @Override
    public String name() {
        return "test-policy";
    }

    @Override
    public FlightAuthorization create(Map<String, String> environment) {
        return new FlightAuthorization() {
            @Override
            public boolean canSeeSchema(String principal, String schema) {
                return "test-user".equals(principal) && "public".equals(schema);
            }

            @Override
            public boolean canReadTable(String principal, String schema, String table) {
                return canSeeSchema(principal, schema) && "orders".equals(table);
            }

        };
    }

    @Override
    public FlightAuthorization create(Map<String, String> environment,
            Function<String, Set<String>> providerGroups) {
        FlightAuthorization base = create(environment);
        return new FlightAuthorization() {
            @Override
            public boolean canSeeSchema(String principal, String schema) {
                return base.canSeeSchema(principal, schema)
                        || (providerGroups.apply(principal).contains("analysts")
                                && "public".equals(schema));
            }

            @Override
            public boolean canReadTable(String principal, String schema, String table) {
                return canSeeSchema(principal, schema) && "orders".equals(table);
            }
        };
    }
}
