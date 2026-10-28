package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests Ranger schema and table policy evaluation. */
@Tag("unit")
class RangerAuthorizationTest {
    /** Requires schema use and table select for every table in a join. */
    @Test
    void checksEveryQueryTableAndKerberosUser() {
        UserGroupInformation.setConfiguration(new Configuration());
        RangerBasePlugin plugin = mock(RangerBasePlugin.class);
        when(plugin.getServiceDef()).thenReturn(mock(RangerServiceDef.class));
        RangerAccessResult allow = mock(RangerAccessResult.class);
        when(allow.getIsAllowed()).thenReturn(true);
        when(plugin.isAccessAllowed(any(RangerAccessRequest.class))).thenReturn(allow);
        RangerAuthorization authorization = new RangerAuthorization(plugin,
                principal -> Set.of("analysts"));

        authorization.requireQuery("alice@EXAMPLE.COM",
                "SELECT * FROM sales.orders o JOIN finance.rates r ON o.id = r.id");

        ArgumentCaptor<RangerAccessRequest> requests =
                ArgumentCaptor.forClass(RangerAccessRequest.class);
        verify(plugin, times(4)).isAccessAllowed(requests.capture());
        assertEquals("alice", requests.getAllValues().get(0).getUser());
        assertEquals(Set.of("analysts"), requests.getAllValues().get(0).getUserGroups());
        assertEquals("use", requests.getAllValues().get(0).getAccessType());
        assertEquals("sales", requests.getAllValues().get(0).getResource().getValue("schema"));
        assertEquals("select", requests.getAllValues().get(1).getAccessType());
        assertEquals("orders", requests.getAllValues().get(1).getResource().getValue("table"));
        assertEquals("finance", requests.getAllValues().get(2).getResource().getValue("schema"));
        assertEquals("rates", requests.getAllValues().get(3).getResource().getValue("table"));
    }

    /** Denies when policies are unavailable, principal is missing, or access is rejected. */
    @Test
    void failsClosed() {
        RangerBasePlugin plugin = mock(RangerBasePlugin.class);
        RangerAuthorization authorization = new RangerAuthorization(plugin,
                principal -> Set.of());
        assertFalse(authorization.canSeeSchema("alice@EXAMPLE.COM", "sales"));
        assertFalse(authorization.canSeeSchema(null, "sales"));
        assertEquals(FlightStatusCode.UNAUTHORIZED,
                assertThrows(FlightRuntimeException.class,
                        () -> authorization.requireQuery("alice@EXAMPLE.COM",
                                "SELECT * FROM sales.orders")).status().code());

        when(plugin.getServiceDef()).thenReturn(mock(RangerServiceDef.class));
        assertFalse(authorization.canReadTable("alice@EXAMPLE.COM", "sales", "orders"));
        assertFalse(authorization.canReadTable("alice@EXAMPLE.COM", "sales", ""));
    }

    /** Denies unqualified tables in Ranger mode. */
    @Test
    void deniesUnqualifiedTable() {
        RangerBasePlugin plugin = mock(RangerBasePlugin.class);
        RangerAuthorization authorization = new RangerAuthorization(plugin,
                principal -> Set.of());
        assertEquals(FlightStatusCode.UNAUTHORIZED,
                assertThrows(FlightRuntimeException.class,
                        () -> authorization.requireQuery("alice@EXAMPLE.COM",
                                "SELECT * FROM orders")).status().code());
    }

    /**
     * Builds a plugin that allows exactly the requests a predicate accepts.
     *
     * @param allows decision on the Ranger user and groups
     * @return mocked plugin
     */
    private static RangerBasePlugin plugin(java.util.function.BiPredicate<String, Set<String>> allows) {
        RangerBasePlugin plugin = mock(RangerBasePlugin.class);
        when(plugin.getServiceDef()).thenReturn(mock(RangerServiceDef.class));
        when(plugin.isAccessAllowed(any(RangerAccessRequest.class))).thenAnswer(call -> {
            RangerAccessRequest request = call.getArgument(0);
            RangerAccessResult result = mock(RangerAccessResult.class);
            when(result.getIsAllowed()).thenReturn(allows.test(request.getUser(), request.getUserGroups()));
            return result;
        });
        return plugin;
    }

    /** Provider groups are added to the mapped groups; without provider groups the mapping alone counts. */
    @Test
    void addsProviderGroupsToMappedGroups() {
        Map<String, Set<String>> provided = new HashMap<>(Map.of("alice", Set.of("bi-users", "analysts")));
        Function<String, Set<String>> groups = RangerAuthorization.union(
                identity -> provided.getOrDefault(identity, Set.of()), identity -> Set.of("analysts", "staff"));
        assertEquals(Set.of("bi-users", "analysts", "staff"), groups.apply("alice"));
        assertEquals(Set.of("analysts", "staff"), groups.apply("bob"));

        RangerAuthorization authorization = new RangerAuthorization(
                plugin((user, userGroups) -> userGroups.contains("bi-users")), groups);
        assertTrue(authorization.canReadTable("alice", "sales", "orders"));
        assertFalse(authorization.canReadTable("bob", "sales", "orders"));
    }

    /** A provider that cannot tell the groups denies instead of evaluating with fewer groups. */
    @Test
    void providerGroupFailureDenies() {
        RangerAuthorization authorization = new RangerAuthorization(plugin((user, userGroups) -> true),
                RangerAuthorization.union(identity -> {
                    throw new IllegalStateException("directory down");
                }, identity -> Set.of()));
        assertFalse(authorization.canSeeSchema("alice", "sales"));
        assertFalse(authorization.canReadTable("alice", "sales", "orders"));
    }

    /**
     * A login name from a password provider (LDAP) gets the decision of the Kerberos principal of the
     * same user: the same Ranger user and, with a provider that adds no groups, the same groups.
     */
    @Test
    void passwordLoginGetsTheKerberosPrincipalsDecision() {
        UserGroupInformation.setConfiguration(new Configuration());
        Map<String, Set<String>> mapping = Map.of("alice", Set.of("analysts"));
        RangerAuthorization authorization = new RangerAuthorization(
                plugin((user, userGroups) -> user.equals("alice") || userGroups.contains("analysts")),
                RangerAuthorization.union(identity -> Set.of(), principal -> mapping.getOrDefault(
                        UserGroupInformation.createRemoteUser(principal).getShortUserName(), Set.of())));
        for (String table : List.of("orders", "rates")) {
            assertEquals(authorization.canReadTable("alice@EXAMPLE.COM", "sales", table),
                    authorization.canReadTable("alice", "sales", table));
        }
        assertTrue(authorization.canReadTable("alice", "sales", "orders"));
        assertFalse(authorization.canReadTable("mallory", "sales", "orders"));
        assertFalse(authorization.canReadTable("mallory@EXAMPLE.COM", "sales", "orders"));
    }
}
