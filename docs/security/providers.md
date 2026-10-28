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
