"""Lane 4 (catalogs and sources), item C3 step 1: Delta Lake test tables written by Delta's own writer.

Run with PySpark 3.5 + delta-spark 3.3 (the Delta jars come from Maven Central via spark.jars.packages):

    python delta-fixtures.py <outputDir>

Writes one Delta table per directory under <outputDir> and <outputDir>/expected.tsv with one line per check:
"<table>\t<where>\t<count>|<sum(id)>|<names>", where <names> is the concatenation of `name` ordered by id
(the shape DuckDbDeltaProbe compares). Every expected line is computed by Spark from the table it describes,
so a reader that agrees with Spark agrees with the line.
"""

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def main(out):
    builder = (SparkSession.builder.master("local[2]").appName("delta-fixtures")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    expected = []

    def path(name):
        return os.path.join(out, name)

    def ddl(name, columns, props="", partition=""):
        spark.sql(f"CREATE TABLE delta.`{path(name)}` ({columns}) USING delta {partition}"
                  + (f" TBLPROPERTIES ({props})" if props else ""))

    def expect(name, where="", version=None):
        source = f"delta.`{path(name)}`" + (f" VERSION AS OF {version}" if version is not None else "")
        row = spark.sql(f"SELECT count(*), sum(id), concat_ws('', transform(array_sort(collect_list("
                        f"named_struct('id', id, 'name', name))), x -> x.name)) FROM {source}"
                        + (f" WHERE {where}" if where else "")).first()
        label = name if version is None else f"{name}@{version}"
        expected.append(f"{label}\t{where}\t{row[0]}|{row[1]}|{row[2]}")

    # Plain table, one commit.
    ddl("t", "id INT, name STRING")
    spark.sql(f"INSERT INTO delta.`{path('t')}` VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    expect("t")
    expect("t", "id = 2")

    # Partitioned (string and int partition columns), several files per partition.
    ddl("part", "id INT, name STRING, p STRING, q INT", partition="PARTITIONED BY (p, q)")
    spark.sql(f"INSERT INTO delta.`{path('part')}` VALUES (1, 'a', 'x', 1), (2, 'b', 'y', 1), (3, 'c', 'x', 2)")
    spark.sql(f"INSERT INTO delta.`{path('part')}` VALUES (4, 'd', 'x', 1), (5, 'e', NULL, NULL)")
    expect("part")
    expect("part", "p = 'x'")
    expect("part", "q = 1")
    expect("part", "p IS NULL")

    # Deletion vectors: DELETE and UPDATE write DVs instead of rewriting files.
    ddl("dv", "id INT, name STRING", "'delta.enableDeletionVectors' = 'true'")
    spark.range(1, 21).selectExpr("CAST(id AS INT) AS id", "chr(96 + CAST(id AS INT)) AS name") \
        .coalesce(1).write.format("delta").mode("append").save(path("dv"))
    spark.sql(f"DELETE FROM delta.`{path('dv')}` WHERE id IN (2, 5, 17)")
    spark.sql(f"UPDATE delta.`{path('dv')}` SET name = 'z' WHERE id = 3")
    expect("dv")
    expect("dv", "id < 6")

    # Column mapping by name: a renamed and a dropped column.
    ddl("cm_name", "id INT, label STRING, extra STRING",
        "'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5'")
    spark.sql(f"INSERT INTO delta.`{path('cm_name')}` VALUES (1, 'a', 'e1'), (2, 'b', 'e2')")
    spark.sql(f"ALTER TABLE delta.`{path('cm_name')}` RENAME COLUMN label TO name")
    spark.sql(f"ALTER TABLE delta.`{path('cm_name')}` DROP COLUMN extra")
    spark.sql(f"INSERT INTO delta.`{path('cm_name')}` VALUES (3, 'c')")
    expect("cm_name")

    # Column mapping by id.
    ddl("cm_id", "id INT, label STRING",
        "'delta.columnMapping.mode' = 'id', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5'")
    spark.sql(f"INSERT INTO delta.`{path('cm_id')}` VALUES (1, 'a'), (2, 'b')")
    spark.sql(f"ALTER TABLE delta.`{path('cm_id')}` RENAME COLUMN label TO name")
    spark.sql(f"INSERT INTO delta.`{path('cm_id')}` VALUES (3, 'c')")
    expect("cm_id")

    # Checkpoints: one every 2 commits, a delete after the last checkpoint.
    ddl("ckpt", "id INT, name STRING", "'delta.checkpointInterval' = '2'")
    for i in range(1, 8):
        spark.sql(f"INSERT INTO delta.`{path('ckpt')}` VALUES ({i}, '{chr(96 + i)}')")
    spark.sql(f"DELETE FROM delta.`{path('ckpt')}` WHERE id = 4")
    expect("ckpt")

    # Versions for time travel: version 1 holds one row, the latest two (and one removed).
    ddl("tt", "id INT, name STRING")
    spark.sql(f"INSERT INTO delta.`{path('tt')}` VALUES (1, 'a')")
    spark.sql(f"INSERT INTO delta.`{path('tt')}` VALUES (2, 'b'), (3, 'c')")
    spark.sql(f"DELETE FROM delta.`{path('tt')}` WHERE id = 1")
    expect("tt")
    expect("tt", version=1)

    # Schema evolution: a column added after the first files.
    ddl("evolved", "id INT, name STRING")
    spark.sql(f"INSERT INTO delta.`{path('evolved')}` VALUES (1, 'a')")
    spark.sql(f"ALTER TABLE delta.`{path('evolved')}` ADD COLUMNS (late STRING)")
    spark.sql(f"INSERT INTO delta.`{path('evolved')}` VALUES (2, 'b', 'l')")
    expect("evolved")
    expect("evolved", "late IS NULL")

    # Types (the probe records what DuckDB makes of them; one NULL row).
    ddl("types", "id INT, name STRING, b BOOLEAN, ti TINYINT, si SMALLINT, bi BIGINT, f FLOAT, d DOUBLE, "
        "dec DECIMAL(18,4), dec38 DECIMAL(38,10), dt DATE, ts TIMESTAMP, ts_ntz TIMESTAMP_NTZ, bin BINARY, "
        "arr ARRAY<INT>, st STRUCT<x: INT, y: STRING>, mp MAP<STRING, INT>")
    spark.sql(f"INSERT INTO delta.`{path('types')}` VALUES (1, 'a', true, -1, 2, 3000000000, 1.5, 2.25, "
              "12345.6789, 1234567890.0123456789, DATE'2024-02-29', TIMESTAMP'2024-02-29 12:34:56.123456', "
              "TIMESTAMP_NTZ'2024-02-29 12:34:56.123456', X'0102', array(1, 2, 3), named_struct('x', 1, 'y', 's'), "
              "map('k', 1))")
    spark.sql(f"INSERT INTO delta.`{path('types')}` VALUES (2, 'b', NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
              "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    expect("types")

    with open(os.path.join(out, "expected.tsv"), "w", encoding="utf-8") as f:
        f.write("\n".join(expected) + "\n")
    for line in expected:
        print("EXPECTED " + line)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
