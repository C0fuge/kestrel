# Prepared schema IPC encoding experiment

The switch `arrowflight.experimental.cachePreparedSchemaMessages` defaults to false.
It reuses immutable protobuf ByteString encodings of already resolved Arrow schemas
in CreatePreparedStatement. Every request still checks access and resolves its current
dataset and parameter schemas before the encoding lookup. No query, result, authorization
decision or prepared handle is cached by this helper.

The producer-local cache holds at most 16 entries and 256 KiB of encoded bytes;
individual values above 64 KiB bypass it. Schema equality includes fields, dictionaries,
nullability and metadata. Serialization happens outside the cache lock, and close
clears the cache. Concurrent misses for the same schema can duplicate serialization
work while an encoding is being inserted.

## Why measure this

Existing small-query JFR recordings show recurring heap allocation beneath
Schema.serializeAsMessage in CreatePreparedStatement. In the last 30 seconds of four
recordings, that subtree accounts for 3.10–3.56% of sampled allocation weight, including
FlatBuffer buffers and byte arrays. This remains present with integer literal reuse
enabled. These are statistical heap-allocation weights, not retained heap or CPU
percentages. Java execution samples in this subtree are too sparse to predict a
throughput gain. Native-method residency is not used as CPU evidence.

[Arrow's schema implementation](https://github.com/apache/arrow-java/blob/v19.0.0/vector/src/main/java/org/apache/arrow/vector/types/pojo/Schema.java)
constructs an IPC message for each serialization call. Reusing the resulting encoding
is an independent allocation hypothesis. The principal risks are schema-key equality,
cache contention and retention; bounds and focused tests address those before screening.

## Validation and experiment

Java 21 compilation, Checkstyle and 74 focused tests passed both with the flag disabled
and enabled. Tests cover exact IPC bytes and round trips, schema/type/metadata/dictionary
differences, bounds, concurrent access, current-schema changes and denied access.
No throughput gain is claimed before cloud A/B measurements.

Compare the same candidate commit with this switch off and on. Keep integer literal
reuse and admission switches off. Use the existing pinned four-core screening with
1, 8 and 32 clients, three alternating pairs for short queries and two for the mixed
workload with a heavy join every eighth query. Preserve all raw query samples, JFR,
resources, gate counters, commands and versions; compare answer checksums and per-kind
tails as well as QPS and process CPU/query. Separate runners require paired ratios
within each workload, rather than comparisons of their absolute QPS.
