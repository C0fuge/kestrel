"""Writes the TPC-H Parquet files of service-tpch.py gen as Iceberg tables (pyiceberg), for the
Iceberg cells of service-stores.yml.

Usage:
  service-iceberg.py hadoop <tpch dir> <warehouse s3://bucket/path> <namespace> <s3 endpoint> <key> <secret>
      Hadoop catalog layout: <warehouse>/<namespace>/<table>/metadata/v1.metadata.json and
      version-hint.text (pyiceberg has no Hadoop catalog: the tables are written through an
      in-memory SQL catalog at those locations, then the metadata file is published under the
      Hadoop name).
  service-iceberg.py rest <tpch dir> <catalog uri> <warehouse name> <namespace> <s3 endpoint> <key> <secret>
      Creates the namespace and tables in an Iceberg REST catalog and appends the data.
"""
import os
import sys

import pyarrow.parquet as pq

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


def s3_props(endpoint, key, secret):
    """FileIO properties of the S3 endpoint (path style)."""
    return {"s3.endpoint": endpoint, "s3.access-key-id": key, "s3.secret-access-key": secret,
            "s3.region": "us-east-1", "s3.path-style-access": "true"}


def write(catalog, tpch, namespace, location=None):
    """Creates the namespace and one table per TPC-H table, appending its Parquet file."""
    catalog.create_namespace(namespace)
    tables = {}
    for t in TABLES:
        data = pq.read_table(os.path.join(tpch, "data", "tpch", t, "data.parquet"))
        kwargs = {"location": f"{location}/{namespace}/{t}"} if location else {}
        table = catalog.create_table(f"{namespace}.{t}", schema=data.schema, **kwargs)
        table.append(data)
        tables[t] = table
        print(f"{namespace}.{t}: {data.num_rows} rows, {table.metadata_location}", flush=True)
    return tables


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "hadoop" and len(sys.argv) == 8:
        from pyiceberg.catalog.sql import SqlCatalog
        tpch, warehouse, namespace, endpoint, key, secret = sys.argv[2:8]
        warehouse = warehouse.rstrip("/")
        catalog = SqlCatalog("w", uri="sqlite:///:memory:", warehouse=warehouse, **s3_props(endpoint, key, secret))
        for t, table in write(catalog, tpch, namespace, warehouse).items():
            io = table.io
            metadata = table.metadata_location
            directory = metadata.rsplit("/", 1)[0]
            with io.new_input(metadata).open() as src:
                body = src.read()
            with io.new_output(directory + "/v1.metadata.json").create(overwrite=True) as dst:
                dst.write(body)
            with io.new_output(directory + "/version-hint.text").create(overwrite=True) as dst:
                dst.write(b"1")
        return 0
    if mode == "rest" and len(sys.argv) == 9:
        from pyiceberg.catalog.rest import RestCatalog
        tpch, uri, warehouse, namespace, endpoint, key, secret = sys.argv[2:9]
        catalog = RestCatalog("r", uri=uri, warehouse=warehouse, **s3_props(endpoint, key, secret))
        write(catalog, tpch, namespace)
        return 0
    sys.exit(__doc__)


if __name__ == "__main__":
    sys.exit(main())
