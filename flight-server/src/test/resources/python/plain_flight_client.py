"""Reads a query through plain pyarrow Flight (no Flight SQL), as generic Flight clients do:
GetSchema and GetFlightInfo on a plain descriptor, then DoGet on every endpoint over the same
connection. Prints one line: rows, endpoints, sum of the first column, and whether GetSchema
and the stream's schema equal the FlightInfo schema.

Usage: plain_flight_client.py <host> <port> <command|path> <sql text or dotted table name>
"""
import sys

import pyarrow.compute as pc
import pyarrow.flight as fl


def main():
    host, port, kind, target = sys.argv[1:5]
    client = fl.connect(f"grpc+tcp://{host}:{port}")
    if kind == "command":
        descriptor = fl.FlightDescriptor.for_command(target.encode("utf-8"))
    else:
        descriptor = fl.FlightDescriptor.for_path(*target.split("/"))
    schema = client.get_schema(descriptor).schema
    info = client.get_flight_info(descriptor)
    rows, total, same = 0, 0, schema.equals(info.schema)
    for endpoint in info.endpoints:
        table = client.do_get(endpoint.ticket).read_all()
        same = same and table.schema.equals(info.schema)
        rows += table.num_rows
        if table.num_rows:
            total += pc.sum(table.column(0)).as_py() or 0
    print(f"rows={rows} endpoints={len(info.endpoints)} sum={total} same_schema={same}")


if __name__ == "__main__":
    main()
