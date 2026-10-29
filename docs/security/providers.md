# Security providers

The server selects authentication and authorization independently. Existing environment settings still select the built-in providers automatically:

| Authentication | Selection | Authorization | Selection |
| --- | --- | --- | --- |
| Static bearer token | `FLIGHT_BEARER_TOKEN` | Unrestricted access | No policy settings |
| Kerberos | `FLIGHT_KERBEROS_*` | Apache Ranger | `RANGER_SERVICE_NAME` |
| Insecure development | `FLIGHT_ALLOW_INSECURE=true` | | |

Set `FLIGHT_AUTH_PROVIDER` to `bearer`, `kerberos`, `insecure`, or a custom provider name to select authentication explicitly. Set `FLIGHT_AUTHORIZATION_PROVIDER` to `allow-all`, `ranger`, or a custom policy provider name. Insecure mode still requires `FLIGHT_ALLOW_INSECURE=true`; it cannot be combined with credentials. Built-in credential variables cannot be mixed with a custom authentication provider. Ranger settings cannot be combined with another policy provider.

An authorization provider that needs individual caller identities cannot run with the shared bearer token or insecure authentication. Custom authentication providers must return `establishesIdentity() == true` to support such policies. Authentication providers that return `requiresTls() == true` cannot start without `FLIGHT_TLS_CERT` and `FLIGHT_TLS_KEY`.

## LDAP / Active Directory logins (`ldap`)

Optional module `flight-auth-ldap` (release asset `flight-auth-<version>-<sha>.tar.zst`, directory `auth/ldap/`):
put `auth/ldap/*` on the server class path and set `FLIGHT_AUTH_PROVIDER=ldap`. Clients log in with a user name and
password (HTTP Basic, as the Flight SQL JDBC driver, ADBC and BI tools do); the provider checks them with an LDAP
simple bind and answers with a signed session bearer token, which the clients send on later calls (no directory
round trip per call; any node with the same `FLIGHT_LDAP_SESSION_SECRET` accepts it). The identity is the login
name, or one attribute of the user's entry; Ranger sees it as it sees a Kerberos principal's short name, with groups
from Hadoop's group mapping. Flight TLS is required.

| Setting | Meaning |
| --- | --- |
| `FLIGHT_LDAP_URL` | Required. `ldaps://host[:636]` or `ldap://host[:389]`; several, space- or comma-separated, tried in order. |
| `FLIGHT_LDAP_START_TLS` | `true`: upgrade `ldap://` connections with StartTLS (refused for `ldaps://`). |
| `FLIGHT_LDAP_ALLOW_PLAINTEXT` | `true`: allow `ldap://` without StartTLS (passwords in clear text; labs only). Default: refused. |
| `FLIGHT_LDAP_TRUSTSTORE`, `_TYPE`, `_PASSWORD` (or `_PASSWORD_FILE`) | Trust store for the directory's certificate (default type PKCS12); default: the JVM's trust. Host names are always checked. |
| `FLIGHT_LDAP_USER_DN_TEMPLATE` | Template mode: the bind name, `{0}` = login name, e.g. `uid={0},ou=people,dc=example,dc=org` or AD's `{0}@corp.example`. |
| `FLIGHT_LDAP_USER_SEARCH_BASE` | Search mode (instead of a template): subtree searched for the user's entry, whose DN is then bound with the password. |
| `FLIGHT_LDAP_USER_SEARCH_FILTER` | Search mode filter, `{0}` = the escaped login name; default `(uid={0})`, AD: `(sAMAccountName={0})`. Exactly one entry must match. |
| `FLIGHT_LDAP_USER_ATTRIBUTE` | Search mode: attribute of the entry used as the identity (e.g. `uid`, `sAMAccountName`); default the login name as sent. |
| `FLIGHT_LDAP_BIND_DN`, `FLIGHT_LDAP_BIND_PASSWORD` (or `_FILE`) | Service account for the user search (search mode) and the group search (both modes); default anonymous searches. A refused service account is reported as `UNAVAILABLE`. |
| `FLIGHT_LDAP_GROUP_SEARCH_BASE` | Optional. Subtree searched for the user's groups; their names are added to the groups Ranger sees (next to Hadoop's group mapping). Unset: Hadoop's group mapping only, as before. |
| `FLIGHT_LDAP_GROUP_SEARCH_FILTER` | Group filter, `{0}` = the escaped identity, `{1}` = the escaped user DN; default `(\|(member={1})(uniqueMember={1})(memberUid={0}))`. AD with nested groups: `(member:1.2.840.113556.1.4.1941:={1})`. |
| `FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE` | Attribute of a group entry that names the group; default `cn` (AD: `cn` or `sAMAccountName`, as Ranger usersync names them). |
| `FLIGHT_LDAP_SESSION_SECRET` | Required. Base64 HMAC key, at least 32 bytes, the same on every node. |
| `FLIGHT_LDAP_SESSION_MINUTES` | Session token lifetime, 1-1440, default 10; after it the client must log in again. |
| `FLIGHT_LDAP_CONNECT_TIMEOUT_MS`, `FLIGHT_LDAP_RESPONSE_TIMEOUT_MS` | Per server connect timeout (default 5000) and per operation timeout (default 10000). |

Login names may contain letters, digits and `. _ - @` only (so no DN or filter escaping issues); empty passwords are
refused before the directory is asked (an empty-password bind is an anonymous bind in LDAP). Wrong credentials are
`UNAUTHENTICATED`; an unreachable directory, a TLS failure or a timeout is `UNAVAILABLE`.

Groups (with `FLIGHT_LDAP_GROUP_SEARCH_BASE`): read at the login (as the service account if one is set, else as the
user) and kept for the session lifetime; a group change takes effect at the next login. A node that receives a
session token without having seen the login reads the groups once, with the service account or anonymously - so on
a cluster set `FLIGHT_LDAP_BIND_DN` unless the directory allows anonymous searches. Groups are never guessed: when
they cannot be read (directory down, the identity's entry not found, more than 1000 groups) the login or call fails
`UNAVAILABLE` and policy checks deny; they never run with fewer groups.

A custom provider can supply groups the same way: `FlightAuthenticationProvider.groups(identity)` (default none) is
added to the Hadoop group mapping for Ranger checks; an exception from it makes the check deny.

## OIDC / Knox JWT bearer tokens (`jwt`)

Optional module `flight-auth-jwt` (same release asset, directory `auth/jwt/`): put `auth/jwt/*` on the server class
path and set `FLIGHT_AUTH_PROVIDER=jwt`. Every call carries a JWT issued by an OIDC provider (Keycloak, Azure AD /
Entra ID, Okta, ...) or Apache Knox as `authorization: Bearer <token>` (Flight SQL JDBC `token=`, ADBC headers); the
provider checks its signature against the issuer's public keys (JWKS) and its issuer, audience, expiry and
not-before, and takes the identity from a claim. Nodes share nothing: no session secret, no login round trip.

| Setting | Meaning |
| --- | --- |
| `FLIGHT_JWT_JWKS_URL` | The issuer's key set, `https://` only (e.g. Keycloak `.../realms/<r>/protocol/openid-connect/certs`, Entra `https://login.microsoftonline.com/<tenant>/discovery/v2.0/keys`). Fetched at the first call, cached, fetched again after `FLIGHT_JWT_JWKS_CACHE_MINUTES` (1-1440, default 15) or when a token names an unknown key id (key rotation), at most once per `FLIGHT_JWT_JWKS_REFRESH_LIMIT_SECONDS` (1-3600, default 30). |
| `FLIGHT_JWT_JWKS_FILE` | Instead of the URL: a local JWK set file read at start (private parts ignored). |
| `FLIGHT_JWT_TRUSTSTORE`, `_TYPE`, `_PASSWORD` (or `_PASSWORD_FILE`) | Trust store for the key set URL's certificate (default type PKCS12); default the JVM's trust. |
| `FLIGHT_JWT_ISSUER` | Required. The exact `iss` value tokens must carry. |
| `FLIGHT_JWT_AUDIENCE` | Required. Accepted audiences, comma- or space-separated; a token's `aud` must contain one. |
| `FLIGHT_JWT_ALGORITHMS` | Accepted signature algorithms; default all of RS256/384/512, PS256/384/512, ES256/384/512. HMAC (`HS*`) and unsigned (`none`) tokens are never accepted. |
| `FLIGHT_JWT_USER_CLAIM` | Claim holding the identity; default `sub`. Use e.g. `preferred_username` (Keycloak), `upn` (AD FS / Entra), whatever matches the user names Ranger knows. |
| `FLIGHT_JWT_GROUPS_CLAIM` | Optional claim listing groups (array of strings or one string); a dotted path reaches into objects, e.g. `realm_access.roles`. Added to the groups Ranger sees (next to Hadoop's group mapping). Keycloak's group mapper: turn off "Full group path" so groups are `analysts`, not `/analysts`. |
| `FLIGHT_JWT_CLOCK_SKEW_SECONDS` | Tolerated clock difference for `exp` / `nbf`, 0-600, default 60. |
| `FLIGHT_JWT_BASIC_PASSWORD` | `true`: also accept a Basic credential whose password is a token (any user name; for tools that only have user name + password fields); the reply carries the token as bearer for the following calls. Default `false`. |
| `FLIGHT_JWT_CONNECT_TIMEOUT_MS`, `FLIGHT_JWT_READ_TIMEOUT_MS` | Key set fetch timeouts (default 5000 / 10000). |

Tokens must carry `iss`, `aud`, `exp` and the user claim; header `typ` may be absent, `JWT` or `at+jwt`. Identities
may contain letters, digits and `. _ - @` only (at most 256). An invalid, expired or foreign token is
`UNAUTHENTICATED`; a key set that cannot be fetched (unreachable, TLS failure, timeout) is `UNAVAILABLE`.

Groups are those of the token: each verified call stores its token's groups for its identity until that token
expires, and policy checks of the identity use them. Two valid tokens of one user with different groups in flight
at the same moment (during a group change at the identity provider) may see each other's groups; a call never runs
with the groups of another user. A group claim of another shape, or with more than 1000 entries, refuses the token.

## Hive Ranger policies (`ranger-hive`)

Optional module `flight-auth-ranger-hive` (same release asset, directory `auth/ranger-hive/`): put
`auth/ranger-hive/*` on the server class path and set `FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive`. The Hive catalog's
schemas and tables are then checked against the cluster's existing **Hive** Ranger service (service type `hive`,
resources database / table / column), so the policies HiveServer2 enforces need not be copied into an
`arrowflight` service. Users and groups are those of the built-in `ranger` provider (short user name; Hadoop group
mapping plus the `ldap` / `jwt` provider's groups).

| Setting | Meaning |
| --- | --- |
| `RANGER_HIVE_SERVICE_NAME` | Required. Name of the Hive service in Ranger admin (e.g. `cl1_hive`). |
| `RANGER_ADMIN_URL` | Required. Ranger admin URL the policies are downloaded from (every 30 s). |
| `RANGER_POLICY_REST_USERNAME`, `RANGER_POLICY_REST_PASSWORD`, `RANGER_POLICY_CACHE_DIR` | As for the `ranger` provider. |
| `RANGER_HIVE_SCHEMA_PREFIX` | The Hive catalog's `schemaPrefix` (default empty): server schema = prefix + Hive database. |

Decisions follow Ranger's Hive authorizer: a schema is visible with any access on the database or inside it (as
`SHOW DATABASES`). The server cannot yet apply masks or row filters or tell which columns a query reads, so a table
is readable only when the caller may read **all of it unmasked and unfiltered**: `select` on all its columns (a
policy on column `*`), no row filter, and neither a mask nor a deny for the caller on any column a policy names.
Otherwise the query is refused (`UNAUTHORIZED`), never answered unmasked. Also refused, because the server cannot
evaluate them exactly: tables with a restrictive policy (deny, allow exception, mask, row filter) for `{OWNER}`
(table owners are not known to the server; `{OWNER}` grants do not apply either) or with conditions, a column
pattern or column exclusion in a restrictive policy, and every table while restrictive tag-based policies exist.
Schemas outside the prefix (other catalogs) are refused. Security zones of the Hive service are honoured (a zone's
policies decide for the tables in it). Each table decision (queries, views, and the table listing of `GetTables`,
which filters by the same check) is audited to the plugin's Ranger audit destinations (`ranger-hive-audit.xml` on
the class path, as for Hive) as one `select` on the table; a table refused because of a column restriction is
audited as denied with the reason and policy id -1. Schema visibility is not audited (as Hive's SHOW filters).
Masks and row filters (query rewrite) are planned; see `docs/dev/PLUGINS-STATUS.md` (P6).

## Add an authentication provider

Implement `pro.surpin.data.arrowflight.server.FlightAuthenticationProviderFactory` with a unique `name()`. Its `create(Map<String, String>)` method returns a `FlightAuthenticationProvider`. The provider can initialize credentials before Hadoop services start and must return an Arrow `CallHeaderAuthenticator` that supplies an authenticated peer identity for each request. Package the factory class in a JAR with this service descriptor:

```text
META-INF/services/pro.surpin.data.arrowflight.server.FlightAuthenticationProviderFactory
```

The descriptor contains the factory's fully qualified class name. Put the JAR on the server classpath and set `FLIGHT_AUTH_PROVIDER` to its name. The server calls the provider's `close()` method during shutdown.

## Add an authorization provider

Implement `pro.surpin.data.arrowflight.server.FlightAuthorizationProviderFactory`. Its `create(Map<String, String>)` method returns a `FlightAuthorization` implementation that checks schema discovery and table reads. The interface's default query check parses SQL and checks every referenced table, including joins. Register it with:

```text
META-INF/services/pro.surpin.data.arrowflight.server.FlightAuthorizationProviderFactory
```

Set `FLIGHT_AUTHORIZATION_PROVIDER` to its name. The server invokes the policy on statement metadata, planning, and every streamed endpoint ticket. The implementation owns any policy client and releases it in `close()`. New providers should deny when their policy backend is unavailable. The factory's `requiresIdentity()` defaults to true; override it only if the provider intentionally supports shared or anonymous identities.

See [Kerberos and Ranger setup](kerberos-ranger.md) for the built-in deployment settings.
