"""Lane 4 (catalogs and sources), item C4 (b) probe: Apache Paimon and Apache Hudi test tables written by their own
Spark writers, for tools/ci/PaimonHudiProbe.java (what can a light reader - DuckDB plus the tables' own metadata,
no Paimon/Hudi libraries - answer correctly?).

Run with PySpark 3.5 and the format's Spark bundle jar (downloaded beforehand, e.g. by Maven):

    python paimon-hudi-fixtures.py paimon <outputDir> <paimonVersion> <paimon-spark-3.5 jar>
    python paimon-hudi-fixtures.py hudi <outputDir> <hudiVersion> <hudi-spark3.5-bundle_2.12 jar>

Paimon: a filesystem catalog warehouse at <outputDir>/paimon (tables at paimon/db.db/<table>). Hudi: one table per
directory under <outputDir>/hudi-<version>. Each run appends to <outputDir>/expected.tsv one line per check:
"<format>/<table>\t<where>\t<count>|<sum(id)>|<names>", where <names> is the concatenation of `name` ordered by id
(the shape of delta-fixtures.py). Every expected line is computed by Spark (snapshot read) from the table it
describes, so a reader that agrees with Spark agrees with the line.
"""

import os
import sys

from pyspark.sql import SparkSession

def session(jar, conf):
    builder = (SparkSession.builder.master("local[2]").appName("paimon-hudi-fixtures")
               .config("spark.jars", jar)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false"))
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def expect(spark, expected, label, source, where=""):
    row = spark.sql(f"SELECT count(*), sum(id), concat_ws('', transform(array_sort(collect_list("
                    f"named_struct('id', id, 'name', name))), x -> x.name)) FROM {source}"
                    + (f" WHERE {where}" if where else "")).first()
    expected.append(f"{label}\t{where}\t{row[0]}|{row[1]}|{row[2]}")


def paimon(out, jar, expected):
    warehouse = os.path.join(out, "paimon")
    spark = session(jar, {
        "spark.sql.catalog.paimon": "org.apache.paimon.spark.SparkCatalog",
        "spark.sql.catalog.paimon.warehouse": "file:" + warehouse,
        "spark.sql.extensions": "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
    })
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.db")

    def t(name):
        return f"paimon.db.{name}"

    def exp(name, where=""):
        expect(spark, expected, "paimon/" + name, t(name), where)

    # Append table (no primary key), partitioned, two commits, then an INSERT OVERWRITE of one partition.
    spark.sql(f"CREATE TABLE {t('app')} (id INT, name STRING, p STRING) PARTITIONED BY (p)"
              " TBLPROPERTIES ('file.format' = 'parquet')")
    spark.sql(f"INSERT INTO {t('app')} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")
    spark.sql(f"INSERT INTO {t('app')} VALUES (4, 'd', 'y'), (5, 'e', NULL)")
    spark.sql(f"INSERT OVERWRITE {t('app')} PARTITION (p = 'y') VALUES (6, 'f')")
    exp("app")
    exp("app", "p = 'x'")

    # Primary-key table (deduplicate merge engine), 2 buckets, upserts and a delete, no compaction: the live files
    # hold several versions of a key in different levels (merge on read).
    spark.sql(f"CREATE TABLE {t('pk')} (id INT, name STRING) TBLPROPERTIES ('primary-key' = 'id',"
              " 'bucket' = '2', 'file.format' = 'parquet', 'num-sorted-run.compaction-trigger' = '100')")
    spark.sql(f"INSERT INTO {t('pk')} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    spark.sql(f"INSERT INTO {t('pk')} VALUES (2, 'B'), (5, 'e')")
    spark.sql(f"DELETE FROM {t('pk')} WHERE id = 3")
    spark.sql(f"UPDATE {t('pk')} SET name = 'D' WHERE id = 4")
    exp("pk")
    exp("pk", "id > 2")

    # Partitioned primary-key table (the key includes the partition column), then a full compaction.
    spark.sql(f"CREATE TABLE {t('pk_compacted')} (id INT, name STRING, p STRING) PARTITIONED BY (p)"
              " TBLPROPERTIES ('primary-key' = 'id,p', 'bucket' = '1', 'file.format' = 'parquet')")
    spark.sql(f"INSERT INTO {t('pk_compacted')} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")
    spark.sql(f"INSERT INTO {t('pk_compacted')} VALUES (1, 'A', 'x'), (4, 'd', 'y')")
    spark.sql(f"DELETE FROM {t('pk_compacted')} WHERE id = 2")
    spark.sql("CALL paimon.sys.compact(table => 'db.pk_compacted')")
    exp("pk_compacted")

    # Primary-key table with deletion vectors.
    spark.sql(f"CREATE TABLE {t('pk_dv')} (id INT, name STRING) TBLPROPERTIES ('primary-key' = 'id',"
              " 'bucket' = '1', 'file.format' = 'parquet', 'deletion-vectors.enabled' = 'true')")
    spark.sql(f"INSERT INTO {t('pk_dv')} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    spark.sql(f"INSERT INTO {t('pk_dv')} VALUES (2, 'B'), (4, 'd')")
    spark.sql(f"DELETE FROM {t('pk_dv')} WHERE id = 1")
    exp("pk_dv")

    # Append table with deletion vectors: DELETE and UPDATE mark rows deleted instead of rewriting files.
    spark.sql(f"CREATE TABLE {t('app_dv')} (id INT, name STRING) TBLPROPERTIES ('file.format' = 'parquet',"
              " 'deletion-vectors.enabled' = 'true')")
    spark.sql(f"INSERT INTO {t('app_dv')} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    spark.sql(f"DELETE FROM {t('app_dv')} WHERE id IN (2, 4)")
    spark.sql(f"UPDATE {t('app_dv')} SET name = 'C' WHERE id = 3")
    exp("app_dv")

    # Schema evolution on an append table: a column added after the first file.
    spark.sql(f"CREATE TABLE {t('evolved')} (id INT, name STRING) TBLPROPERTIES ('file.format' = 'parquet')")
    spark.sql(f"INSERT INTO {t('evolved')} VALUES (1, 'a')")
    spark.sql(f"ALTER TABLE {t('evolved')} ADD COLUMNS (late STRING)")
    spark.sql(f"INSERT INTO {t('evolved')} VALUES (2, 'b', 'l')")
    exp("evolved")
    exp("evolved", "late IS NULL")
    spark.stop()


def hudi(out, version, jar, expected):
    base = os.path.join(out, f"hudi-{version}")
    spark = session(jar, {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
    })

    def ddl(name, kind, partition="", extra=""):
        spark.sql(f"CREATE TABLE {name} (id INT, name STRING, ts BIGINT, p STRING) USING hudi"
                  + (f" PARTITIONED BY ({partition})" if partition else "")
                  + f" TBLPROPERTIES (type = '{kind}', primaryKey = 'id', preCombineField = 'ts'"
                  + " , 'hoodie.metadata.enable' = 'false'" + extra + ")"
                  + f" LOCATION 'file:{os.path.join(base, name)}'")

    def exp(name, where=""):
        spark.read.format("hudi").load("file:" + os.path.join(base, name)).createOrReplaceTempView("v_" + name)
        expect(spark, expected, f"hudi-{version}/{name}", "v_" + name, where)

    # Copy-on-write, partitioned: insert, upsert, delete (each rewrites the touched file groups).
    ddl("cow", "cow", "p")
    spark.sql("INSERT INTO cow VALUES (1, 'a', 1, 'x'), (2, 'b', 1, 'y'), (3, 'c', 1, 'x')")
    spark.sql("INSERT INTO cow VALUES (2, 'B', 2, 'y'), (4, 'd', 2, 'x')")
    spark.sql("DELETE FROM cow WHERE id = 3")
    spark.sql("UPDATE cow SET name = 'A', ts = 3 WHERE id = 1")
    exp("cow")
    exp("cow", "p = 'x'")

    # Copy-on-write, INSERT OVERWRITE of one partition (a replacecommit replacing file groups).
    ddl("cow_overwrite", "cow", "p")
    spark.sql("INSERT INTO cow_overwrite VALUES (1, 'a', 1, 'x'), (2, 'b', 1, 'y')")
    spark.sql("INSERT INTO cow_overwrite VALUES (3, 'c', 1, 'y')")
    spark.sql("INSERT OVERWRITE cow_overwrite PARTITION (p = 'y') SELECT 5, 'e', 2")
    exp("cow_overwrite")

    # Copy-on-write with a commit left inflight: a failed write must stay invisible. Simulated by a write whose
    # completed instant file is removed afterwards (what a crash between data and commit leaves).
    ddl("cow_inflight", "cow")
    spark.sql("INSERT INTO cow_inflight VALUES (1, 'a', 1, 'x'), (2, 'b', 1, 'x')")
    exp("cow_inflight")
    spark.sql("INSERT INTO cow_inflight VALUES (2, 'B', 2, 'x'), (3, 'c', 2, 'x')")

    # Merge-on-read: updates and a delete go to log files (no compaction).
    ddl("mor", "mor", "p", ", 'hoodie.compact.inline' = 'false'")
    spark.sql("INSERT INTO mor VALUES (1, 'a', 1, 'x'), (2, 'b', 1, 'y'), (3, 'c', 1, 'x')")
    spark.sql("INSERT INTO mor VALUES (2, 'B', 2, 'y'), (4, 'd', 2, 'x')")
    spark.sql("DELETE FROM mor WHERE id = 3")
    exp("mor")

    # Merge-on-read compacted inline after every delta commit: the update goes to a log file, the compaction
    # that follows writes a new base file, so the last slice is a base file only.
    ddl("mor_compacted", "mor", "", ", 'hoodie.compact.inline' = 'true',"
        " 'hoodie.compact.inline.max.delta.commits' = '1'")
    spark.sql("INSERT INTO mor_compacted VALUES (1, 'a', 1, 'x'), (2, 'b', 1, 'x'), (3, 'c', 1, 'x')")
    spark.sql("UPDATE mor_compacted SET name = 'B', ts = 2 WHERE id = 2")
    spark.sql("DELETE FROM mor_compacted WHERE id = 3")
    exp("mor_compacted")
    spark.stop()
    drop_last_completed(os.path.join(base, "cow_inflight"))


def drop_last_completed(table):
    """Removes the newest completed commit file of a Hudi table, leaving its data files and inflight files."""
    hoodie = os.path.join(table, ".hoodie")
    timeline = os.path.join(hoodie, "timeline")
    folder = timeline if os.path.isdir(timeline) else hoodie
    done = sorted(f for f in os.listdir(folder) if f.endswith(".commit") and not f.startswith("."))
    os.remove(os.path.join(folder, done[-1]))
    print("REMOVED " + os.path.join(folder, done[-1]))


def main(kind, out, version, jar):
    os.makedirs(out, exist_ok=True)
    expected = []
    if kind == "paimon":
        paimon(out, jar, expected)
    else:
        hudi(out, version, jar, expected)
    with open(os.path.join(out, "expected.tsv"), "a", encoding="utf-8") as f:
        f.write("\n".join(expected) + "\n")
    for line in expected:
        print("EXPECTED " + line)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
