package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Probes edge cases where our fork and upstream overlap. It records outcomes (printed as
 * "EDGE|case|outcome") instead of asserting one side's behaviour, so it runs on both trials.
 */
@Tag("unit")
class OverlapEdgeCaseTest {

    private static void report(String name, String outcome) {
        System.out.println("EDGE|" + name + "|" + outcome);
    }

    private static String authOutcome(CallHeaderAuthenticator authenticator, String header) {
        CallHeaders headers = new FlightCallHeaders();
        if (header != null) {
            headers.insert("Authorization", header);
        }
        try {
            return "accepted peer=" + authenticator.authenticate(headers).getPeerIdentity();
        } catch (RuntimeException e) {
            return "rejected " + e.getClass().getSimpleName();
        }
    }

    /** Header forms real clients send: JDBC user/password is Basic, token= is Bearer. */
    @Test
    void authenticationHeaderForms() throws Exception {
        String basic = "Basic " + Base64.getEncoder().encodeToString(
                "user:secret".getBytes(StandardCharsets.UTF_8));
        Map<String, CallHeaderAuthenticator> providers = new java.util.LinkedHashMap<>();
        providers.put("upstream-bearer", FlightSecurity.bearerAuthenticator("secret"));
        providers.put("ours-token", new pro.surpin.data.arrowflight.server.adapters
                .TokenAuthenticationProviderFactory().create(Map.of("FLIGHT_AUTH_TOKEN", "secret"))
                .authenticator());
        for (Map.Entry<String, CallHeaderAuthenticator> p : providers.entrySet()) {
            report("auth." + p.getKey() + ".bearer", authOutcome(p.getValue(), "Bearer secret"));
            report("auth." + p.getKey() + ".bearer-trailing-space",
                    authOutcome(p.getValue(), "Bearer secret "));
            report("auth." + p.getKey() + ".basic-password", authOutcome(p.getValue(), basic));
            report("auth." + p.getKey() + ".none", authOutcome(p.getValue(), null));
            report("auth." + p.getKey() + ".wrong", authOutcome(p.getValue(), "Bearer nope"));
        }
        // Startup rules: what a deployment without TLS can run.
        for (Map<String, String> env : List.of(Map.<String, String>of(),
                Map.of("FLIGHT_BEARER_TOKEN", "secret"),
                Map.of("FLIGHT_ALLOW_INSECURE", "true"),
                Map.of("FLIGHT_AUTH_PROVIDER", "token", "FLIGHT_AUTH_TOKEN", "secret"))) {
            String outcome;
            try {
                outcome = "starts " + FlightSecurity.fromSettings(env);
            } catch (RuntimeException e) {
                outcome = "refuses: " + e.getMessage();
            }
            report("startup.plaintext." + env.keySet(), outcome);
        }
    }

    /** Which tables the SQL-based authorization check sees for SQL complete-answer mode accepts. */
    @Test
    void authorizationSeesTablesOfCompleteAnswerSql() {
        String[] queries = {
            "SELECT * FROM s.a",
            "SELECT * FROM s.a WHERE x IN (SELECT y FROM s.secret)",
            "SELECT * FROM s.a, s.secret",
            "WITH t AS (SELECT * FROM s.secret) SELECT * FROM t",
            "SELECT * FROM s.a UNION ALL SELECT * FROM s.secret",
            "SELECT * FROM read_parquet('/data/parquet/s/secret/*.parquet')",
            "SELECT (SELECT max(y) FROM s.secret) AS m FROM s.a",
            "SELECT * FROM a",
        };
        for (String q : queries) {
            List<String> seen = new ArrayList<>();
            FlightAuthorization recorder = new FlightAuthorization() {
                @Override
                public boolean canSeeSchema(String principal, String schema) {
                    return true;
                }

                @Override
                public boolean canReadTable(String principal, String schema, String table) {
                    seen.add(schema + "." + table);
                    return !"secret".equals(table);
                }
            };
            String outcome;
            try {
                recorder.requireQuery("alice", q);
                outcome = "ALLOWED";
            } catch (org.apache.arrow.flight.FlightRuntimeException e) {
                outcome = "denied " + e.status().code();
            } catch (RuntimeException e) {
                outcome = "error " + e.getClass().getSimpleName();
            }
            report("authz[" + q + "]", outcome + " checked=" + seen);
        }
    }

    /** LIKE filter edge cases of the metadata calls. */
    @Test
    void likeFilterEdgeCases() throws Exception {
        java.lang.reflect.Method m = pro.surpin.data.arrowflight.server.adapters.ParquetAdapter.class
                .getDeclaredMethod("createLikePredicate", String.class);
        m.setAccessible(true);
        String[][] cases = {
            {"", "lineitem"}, {"", ""}, {"line_tem", "lineitem"}, {"line\\_item", "line_item"},
            {"line\\_item", "lineXitem"}, {"%", "a\nb"}, {"a%b", "a\nb"}, {"LINEITEM", "lineitem"},
            {"a.c", "abc"}, {"[ab]", "a"},
        };
        for (String[] c : cases) {
            @SuppressWarnings("unchecked")
            java.util.function.Predicate<String> p =
                    (java.util.function.Predicate<String>) m.invoke(null, c[0]);
            report("like[" + c[0].replace("\n", "\\n") + " ~ " + c[1].replace("\n", "\\n") + "]",
                    String.valueOf(p.test(c[1])));
        }
    }
}
