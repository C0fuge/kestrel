# Integer literal reuse: coercion counterexample

The safe server integration excludes this rewrite and its research-only adapter
tests. Its historical experimental properties are effective only on the pinned
research sources below. Benchmark collectors and oracle checks remain independent.

The integer-literal execution rewrite in research revision `437c0f4f` is blocked
for integration. A successful DuckDB 1.5.6 query becomes an execution failure when
its literal is replaced by the actual `literalParameters` helper and bound through
the production `bindParameters` method. The experiment defaults to off; this finding
concerns its opt-in execution path. A successful default-off CI run cannot validate
that path's semantic equivalence.

DuckDB 1.5 documents separate binding/conversion rules for
[integer literals](https://duckdb.org/docs/1.5/sql/data_types/literal_types#integer-literals):
a literal can convert to an integer type that fits its value. The counterexample
below directly tests the resulting difference from a typed INTEGER parameter.

## Two-row native reproduction

Use an in-memory DuckDB 1.5.6 JDBC connection on Java 21. No files, extensions,
external I/O or user data are needed:

```sql
CREATE SCHEMA s;
CREATE TABLE s.t(a UHUGEINT);
INSERT INTO s.t VALUES (0), ('9223372036854775808'::UHUGEINT);

SELECT CAST(a AS VARCHAR) AS v, typeof(a) AS t
FROM s.t WHERE a = 0 ORDER BY v, t;
```

The original query returns one row, `0 | UHUGEINT`. Both projected columns have
VARCHAR metadata. The actual helper accepts this SQL, produces values `[0]` with
Java type Integer, and replaces the predicate with:

```sql
WHERE a = CAST(? AS INTEGER)
```

Preparing that SQL and invoking the actual production binder, including its
parameter-count check, clearParameters and setObject calls, causes executeQuery to
fail. DuckDB reports that UINT128 value `9223372036854775808` cannot be cast to INT64.
With the high row changed to `9223372036854775807`, both executions succeed.
Thus 2^63 already exposes the failure; it does not require the maximum UHUGEINT.

## Ordinary VARCHAR source columns also expose it

The difference does not require a persisted UHUGEINT column or exporting one in a
result. An ordinary string table and explicit predicate cast suffice:

```sql
CREATE TABLE s.text_values(a VARCHAR);
INSERT INTO s.text_values VALUES ('0'), ('9223372036854775808');

SELECT a, typeof(a) AS t
FROM s.text_values WHERE CAST(a AS UHUGEINT) = 0 ORDER BY a;
```

The original returns `0 | VARCHAR`. The helper accepts the SQL and changes only
the right-hand side to `CAST(? AS INTEGER)`. Binding Integer zero then fails with
the same UINT128-to-INT64 conversion error. The alternative predicate
`a::UHUGEINT = 0` also reproduces it. Both result columns remain VARCHAR, so this
is predicate behavior rather than unsupported result decoding.

## Extended fixed-fixture probe

The probe compiled the actual DuckDbAdapter, DescribeKey and TimeTravelClause
sources from `437c0f4f`, with their StreamCpuCounters dependency, using cached Java
21 dependencies. It called the actual shape helper and the private production
binder reflectively; no substitute rewriter or binder was used for the final
comparison. DuckDB version was checked as 1.5.6. Each fixture was a tiny populated
in-memory table; no throughput or timing measurement was performed.

Thirteen fixtures covered UNION(a UTINYINT, b VARCHAR), UNION(a TINYINT, b VARCHAR),
ENUM with numeric labels, ENUM with numeric and nonnumeric labels, UHUGEINT,
BIGNUM, UUID, TIME, numeric JSON, mixed JSON, STRUCT, LIST and MAP. The probes used
`=`, `!=`, `<`, `<=`, `>` and `>=`, with values 0, 1, 2, 10, 255, 256 and 999999999,
also testing unary plus and unary minus.

For successful queries the probe compared ordered rows and complete projected
metadata: column labels, type names, JDBC types, precision and scale. It selected
CAST(a AS VARCHAR) and typeof(a) so exotic source types could be read safely.
Failures retained their messages and prepare/bind/execute stages; two refusals
were counted alike without claiming identical diagnostics or failure stages.

| Final actual-helper/binder result | Cases |
| --- | --- |
| Eligible transformed comparisons | 1,092 |
| Both succeed with equal rows and projected metadata | 196 |
| Both refuse the query | 848 |
| Original succeeds, transformed query fails | 48 |
| Unary-plus forms rejected by the helper, original path retained | 546 |

All 48 differences occurred in the UHUGEINT fixture, which included small values
and the maximum unsigned 128-bit value. Every comparison operator exposed the
problem with nonnegative literals, including unary minus zero. UNION narrow-member
comparisons refused on both paths, although the refusal stage could differ.
Numeric ENUM and JSON fixtures and BIGNUM supplied successful comparisons; adding
nonnumeric labels or JSON values also exercised conversion refusals. These finite
checks are not a proof for every type, value, expression or optimizer path.

A separate two-row boundary check covered UHUGEINT, UBIGINT, HUGEINT, BIGINT and
DECIMAL(38,0), skipping values outside each type's range. UBIGINT at its maximum,
HUGEINT at its maximum and the checked DECIMAL values matched. The UHUGEINT
failure began above the signed 64-bit maximum. An initial diagnostic driver
stopped after reusing a JDBC Statement closed by an unsupported fixture cast;
that log was retained, and the boundary check completed with fresh setup
Statements. It did not alter the coercion result.

## Decision and retained evidence

Retire the execution rewrite through the original checked-SQL path until
literal-versus-typed-parameter equivalence is established with type information.
Do not patch lexical eligibility with a type-name blacklist: expressions over
otherwise ordinary columns already reproduce the problem. Keeping a casted
parameter's apparent INTEGER type is insufficient evidence that DuckDB will apply
the same coercion as it does to an integer literal.

The pinned short/mixed and TPC-H experiments remain historical measurements of
their specific queries and values. Preserve their raw archives, manifests,
per-query answers and profiles. Their successful answer checks and performance
ratios do not remove this counterexample, and a successful default-off full CI run
is not evidence that the enabled candidate is safe.

The private native-probe evidence retains source hashes, the actual helper/binder
matrix, original and rewritten SQL, bound values, typed rows, metadata, errors,
minimal expression proofs and complete boundary diagnostics. No production source
change or lexical workaround accompanies this report.
