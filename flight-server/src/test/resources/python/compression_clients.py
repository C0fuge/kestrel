"""Reads one query through pyarrow Flight (C++) and the ADBC Flight SQL driver (Go), plain and
with each arrow-compression codec, in both result modes, and checks every answer equals the
plain one of the same client. Prints one line per read; exits non-zero on a mismatch.

Usage: compression_clients.py <host> <port> <sql> <expected rows> <pyarrow|adbc-go>
"""
import sys

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow as pa
import pyarrow.flight as fl

CODECS = (None, "none", "zstd", "lz4")
MODES = ("partial", "complete")
COMMAND_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"


def varint(n):
    """Encodes a protobuf varint."""
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def field(number, payload):
    """Encodes a length-delimited protobuf field."""
    return varint(number << 3 | 2) + varint(len(payload)) + payload


def statement(sql):
    """Builds the Any-wrapped CommandStatementQuery for a query (pyarrow has no Flight SQL)."""
    return field(1, COMMAND_TYPE) + field(2, field(1, sql.encode("utf-8")))


def canonical(table):
    """Sorts a table by all its sortable columns so answers of several endpoints compare."""
    keys = [(f.name, "ascending") for f in table.schema
            if not pa.types.is_nested(f.type)]
    return table.combine_chunks().sort_by(keys)


def read_pyarrow(host, port, sql, mode, codec):
    """Plans with GetFlightInfo and reads every endpoint with DoGet through pyarrow."""
    client = fl.connect(f"grpc+tcp://{host}:{port}")
    plan = fl.FlightCallOptions(headers=[(b"x-flight-result-mode", mode.encode())])
    info = client.get_flight_info(fl.FlightDescriptor.for_command(statement(sql)), plan)
    headers = [] if codec is None else [(b"arrow-compression", codec.encode())]
    tables = []
    for endpoint in info.endpoints:
        reader = client.do_get(endpoint.ticket, fl.FlightCallOptions(headers=headers))
        tables.append(reader.read_all())
    client.close()
    return pa.concat_tables(tables)


def read_adbc(host, port, sql, mode, codec):
    """Runs the query through the ADBC Flight SQL driver with the headers as call header options."""
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": mode}
    if codec is not None:
        kwargs["adbc.flight.sql.rpc.call_header.arrow-compression"] = codec
    with flightsql.connect(f"grpc://{host}:{port}", db_kwargs=kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetch_arrow_table()


def main():
    host, port, sql, expected = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4])
    name = sys.argv[5]
    read = {"pyarrow": read_pyarrow, "adbc-go": read_adbc}[name]
    failures = 0
    for mode in MODES:
        plain = None
        for codec in CODECS:
            table = canonical(read(host, port, sql, mode, codec))
            if plain is None:
                plain = table
            same = table.num_rows == expected and table.equals(plain)
            failures += 0 if same else 1
            print(f"{name} {mode} {codec or '-'} rows={table.num_rows} "
                  f"{'same' if same else 'DIFFERENT'}", flush=True)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
