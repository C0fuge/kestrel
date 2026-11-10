"""Per-RPC latency of small queries against one server (item 40 (b), lane 4).

Usage: small-query-rpc.py <host:port> <queries per class> [classes JSON]

Sends each query class with a fresh random key ({k} in the SQL) one at a time, in complete
mode, over plain Arrow Flight (no ADBC), and prints the p50 of every call in two flows:
the statement flow (GetFlightInfo(CommandStatementQuery), DoGet) and the prepared flow ADBC
uses (CreatePreparedStatement, GetFlightInfo(CommandPreparedStatementQuery), DoGet,
ClosePreparedStatement). ClosePreparedStatement does no work on the server, so its time is the
round trip of a call; the rest of each call beyond it is the server's own time. The first
tenth of every class is a warm-up and not counted. The default classes are the point cell's
(``point_orders``, ``point_lineitem``, ``count_filter``) plus ``const`` (no table).
"""
import json
import random
import statistics
import sys
import time

import pyarrow.flight as fl

SQL_TYPE = "type.googleapis.com/arrow.flight.protocol.sql."
CLASSES = {
    "point_orders": "SELECT * FROM tpch.orders WHERE o_orderkey = {k}",
    "point_lineitem": "SELECT * FROM tpch.lineitem WHERE l_orderkey = {k}",
    "count_filter": "SELECT count(*) FROM tpch.orders WHERE o_custkey = {k} % 15000 AND o_totalprice > 1000",
    "const": "SELECT {k} AS x",
}


def varint(n):
    """Encodes a protobuf varint."""
    out = bytearray()
    while True:
        low, n = n & 0x7F, n >> 7
        out.append(low | (0x80 if n else 0))
        if not n:
            return bytes(out)


def field(number, payload):
    """Encodes a length-delimited protobuf field."""
    return bytes([(number << 3) | 2]) + varint(len(payload)) + payload


def packed(type_name, message):
    """Wraps a Flight SQL message in google.protobuf.Any."""
    return field(1, (SQL_TYPE + type_name).encode()) + field(2, message)


def fields(buf):
    """Decodes the length-delimited fields of a protobuf message (others are skipped)."""
    out, i = {}, 0
    while i < len(buf):
        key, shift = 0, 0
        while True:
            b = buf[i]
            i += 1
            key |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
        if key & 7 == 2:
            length, shift = 0, 0
            while True:
                b = buf[i]
                i += 1
                length |= (b & 0x7F) << shift
                shift += 7
                if not b & 0x80:
                    break
            out[key >> 3] = buf[i:i + length]
            i += length
        else:
            while buf[i] & 0x80:
                i += 1
            i += 1
    return out


def p50(values):
    """Returns the median after the warm-up tenth, in ms."""
    return statistics.median(values[len(values) // 10:]) * 1000.0


def main():
    server, n = sys.argv[1], int(sys.argv[2])
    classes = json.loads(sys.argv[3]) if len(sys.argv) > 3 else CLASSES
    client = fl.FlightClient(f"grpc://{server}")
    opts = fl.FlightCallOptions(headers=[(b"x-flight-result-mode", b"complete")])
    rnd = random.Random(1)
    for name, template in classes.items():
        info_t, get_t = [], []
        for _ in range(n):
            sql = template.format(k=rnd.randint(1, 1500000) * 4 - 3)
            t0 = time.perf_counter()
            info = client.get_flight_info(fl.FlightDescriptor.for_command(
                packed("CommandStatementQuery", field(1, sql.encode()))), opts)
            t1 = time.perf_counter()
            client.do_get(info.endpoints[0].ticket, opts).read_all()
            info_t.append(t1 - t0)
            get_t.append(time.perf_counter() - t1)
        print(f"{name} statement getFlightInfo={p50(info_t):.2f} doGet={p50(get_t):.2f}")
        times = {"create": [], "getFlightInfo": [], "doGet": [], "close": []}
        for _ in range(n):
            sql = template.format(k=rnd.randint(1, 1500000) * 4 - 3)
            t0 = time.perf_counter()
            result = list(client.do_action(fl.Action("CreatePreparedStatement", packed(
                "ActionCreatePreparedStatementRequest", field(1, sql.encode()))), opts))
            handle = fields(fields(result[0].body.to_pybytes())[2])[1]
            t1 = time.perf_counter()
            info = client.get_flight_info(fl.FlightDescriptor.for_command(
                packed("CommandPreparedStatementQuery", field(1, handle))), opts)
            t2 = time.perf_counter()
            client.do_get(info.endpoints[0].ticket, opts).read_all()
            t3 = time.perf_counter()
            list(client.do_action(fl.Action("ClosePreparedStatement", packed(
                "ActionClosePreparedStatementRequest", field(1, handle))), opts))
            t4 = time.perf_counter()
            for key, (a, b) in zip(times, ((t0, t1), (t1, t2), (t2, t3), (t3, t4))):
                times[key].append(b - a)
        print(f"{name} prepared " + " ".join(f"{k}={p50(v):.2f}" for k, v in times.items()))


if __name__ == "__main__":
    main()
