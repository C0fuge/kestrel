package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/**
 * ORC files of a fixed twelve-column table for the plugin's and the server's tests (the server's
 * run it in the metastore's class loader, where orc-core is): row r of {@link #EXPECTED} over
 * range(from, to) is written as row r of the file.
 */
public final class OrcTestFiles {

    /** The table's columns in the metastore's notation. */
    public static final String COLUMNS = "i int;b bigint;s string;dec decimal(12,2);dt date;"
            + "d double;flag boolean;ch char(4);ts timestamp;arr array<int>;"
            + "st struct<x:int,y:string>;m map<string,int>";
    /** The same rows computed by DuckDB: append {@code FROM range(from, to)}. */
    public static final String EXPECTED = "SELECT range::INTEGER AS i,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE range * 1000000000 END AS b,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE 's' || range || 'ü' END AS s,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE (range * 1.25)::DECIMAL(12,2) END AS dec,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE DATE '2020-01-01' + range::INTEGER END AS dt,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE range::DOUBLE / 8 END AS d,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE range % 2 = 0 END AS flag,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE 'c' || (range % 10) END AS ch,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE TIMESTAMP '2020-06-01 01:30:00'"
            + " + INTERVAL (range) MINUTE END AS ts,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE [range::INTEGER, range::INTEGER + 1] END AS arr,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE {'x': -range::INTEGER, 'y': 'y' || range}"
            + " END AS st,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE MAP {'k': range::INTEGER} END AS m";
    private static final String STRUCT = "struct<i:int,b:bigint,s:string,dec:decimal(12,2),"
            + "dt:date,d:double,flag:boolean,ch:char(4),ts:timestamp,arr:array<int>,"
            + "st:struct<x:int,y:string>,m:map<string,int>>";

    /** A TPC-H lineitem-like table in the metastore's notation (perf tests). */
    public static final String LINEITEM_COLUMNS = "l_orderkey bigint;l_partkey bigint;"
            + "l_suppkey bigint;l_linenumber int;l_quantity decimal(15,2);"
            + "l_extendedprice decimal(15,2);l_discount decimal(15,2);l_tax decimal(15,2);"
            + "l_returnflag string;l_linestatus string;l_shipdate date;l_commitdate date;"
            + "l_receiptdate date;l_shipinstruct string;l_shipmode string;l_comment string";
    /** The lineitem-like rows computed by DuckDB: append {@code FROM range(from, to) t(r)}. */
    public static final String LINEITEM = "SELECT r // 4 AS l_orderkey,"
            + " (r * 7919) % 200000 + 1 AS l_partkey, (r * 104729) % 10000 + 1 AS l_suppkey,"
            + " (r % 4 + 1)::INTEGER AS l_linenumber,"
            + " (((r * 31) % 50 + 1) * 100 * 0.01)::DECIMAL(15,2) AS l_quantity,"
            + " (((r * 2654435761) % 10000000 + 90000) * 0.01)::DECIMAL(15,2) AS l_extendedprice,"
            + " (r % 11 * 0.01)::DECIMAL(15,2) AS l_discount,"
            + " (r % 9 * 0.01)::DECIMAL(15,2) AS l_tax,"
            + " ['A','N','R'][r % 3 + 1] AS l_returnflag, ['O','F'][r % 2 + 1] AS l_linestatus,"
            + " DATE '1992-01-01' + ((r * 7907) % 2500)::INTEGER AS l_shipdate,"
            + " DATE '1992-01-01' + ((r * 7927) % 2500)::INTEGER AS l_commitdate,"
            + " DATE '1992-01-01' + ((r * 7933) % 2500)::INTEGER AS l_receiptdate,"
            + " ['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'][r % 4 + 1]"
            + " AS l_shipinstruct,"
            + " ['AIR','MAIL','SHIP','TRUCK','RAIL','FOB','REG AIR'][r % 7 + 1] AS l_shipmode,"
            + " 'carefully ' || ['final','regular','express','pending'][(r * 13) % 4 + 1]"
            + " || ' deposits ' || ((r * 48271) % 100000)::VARCHAR AS l_comment";
    private static final String LINEITEM_STRUCT = "struct<l_orderkey:bigint,l_partkey:bigint,"
            + "l_suppkey:bigint,l_linenumber:int,l_quantity:decimal(15,2),"
            + "l_extendedprice:decimal(15,2),l_discount:decimal(15,2),l_tax:decimal(15,2),"
            + "l_returnflag:string,l_linestatus:string,l_shipdate:date,l_commitdate:date,"
            + "l_receiptdate:date,l_shipinstruct:string,l_shipmode:string,l_comment:string>";
    private static final byte[][] FLAGS = bytes("A", "N", "R");
    private static final byte[][] STATUSES = bytes("O", "F");
    private static final byte[][] INSTRUCTIONS = bytes("DELIVER IN PERSON", "COLLECT COD", "NONE",
            "TAKE BACK RETURN");
    private static final byte[][] MODES = bytes("AIR", "MAIL", "SHIP", "TRUCK", "RAIL", "FOB",
            "REG AIR");
    private static final String[] WORDS = {"final", "regular", "express", "pending"};
    private static final long EPOCH_1992 = LocalDate.of(1992, 1, 1).toEpochDay();

    private OrcTestFiles() {
    }

    /**
     * Writes rows [from, to) of the lineitem-like table ({@link #LINEITEM}) as an ORC file with
     * ORC's default stripe and row group sizes.
     *
     * @param file file to write
     * @param from first row
     * @param to row after the last
     * @param compression ORC compression kind (NONE, ZLIB, SNAPPY, LZ4, LZO, ZSTD)
     * @throws IOException when it cannot be written
     */
    public static void writeLineitem(String file, long from, long to, String compression)
            throws IOException {
        Path path = Path.of(file);
        Files.createDirectories(path.getParent());
        TypeDescription schema = TypeDescription.fromString(LINEITEM_STRUCT);
        try (Writer writer = OrcFile.createWriter(new org.apache.hadoop.fs.Path(path.toUri()),
                OrcFile.writerOptions(new Configuration(false)).setSchema(schema)
                        .compress(CompressionKind.valueOf(compression.toUpperCase(Locale.ROOT))))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            for (long r = from; r < to; r++) {
                fillLineitem(batch.cols, batch.size++, r);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
            }
        }
    }

    /**
     * Writes rows [from, to) as an ORC file with ORC's default compression, row groups of 1000.
     *
     * @param file file to write
     * @param from first row
     * @param to row after the last
     * @throws IOException when it cannot be written
     */
    public static void write(Path file, int from, int to) throws IOException {
        write(file.toString(), from, to, "ZLIB");
    }

    /**
     * Writes rows [from, to) as an ORC file, row groups of 1000.
     *
     * @param file file to write
     * @param from first row
     * @param to row after the last
     * @param compression ORC compression kind (NONE, ZLIB, SNAPPY, LZ4, LZO, ZSTD)
     * @throws IOException when it cannot be written
     */
    public static void write(String file, int from, int to, String compression) throws IOException {
        Path path = Path.of(file);
        Files.createDirectories(path.getParent());
        TypeDescription schema = TypeDescription.fromString(STRUCT);
        try (Writer writer = OrcFile.createWriter(new org.apache.hadoop.fs.Path(path.toUri()),
                OrcFile.writerOptions(new Configuration(false)).setSchema(schema)
                        .compress(CompressionKind.valueOf(compression.toUpperCase(Locale.ROOT)))
                        .rowIndexStride(1000))) {
            VectorizedRowBatch batch = schema.createRowBatch(500);
            for (int r = from; r < to; r++) {
                fill(batch.cols, batch.size++, r);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
            }
        }
    }

    private static void fillLineitem(ColumnVector[] cols, int row, long r) {
        ((LongColumnVector) cols[0]).vector[row] = r / 4;
        ((LongColumnVector) cols[1]).vector[row] = r * 7919 % 200000 + 1;
        ((LongColumnVector) cols[2]).vector[row] = r * 104729 % 10000 + 1;
        ((LongColumnVector) cols[3]).vector[row] = r % 4 + 1;
        ((DecimalColumnVector) cols[4]).vector[row].setFromLongAndScale((r * 31 % 50 + 1) * 100, 2);
        ((DecimalColumnVector) cols[5]).vector[row]
                .setFromLongAndScale(r * 2654435761L % 10000000 + 90000, 2);
        ((DecimalColumnVector) cols[6]).vector[row].setFromLongAndScale(r % 11, 2);
        ((DecimalColumnVector) cols[7]).vector[row].setFromLongAndScale(r % 9, 2);
        ((BytesColumnVector) cols[8]).setRef(row, FLAGS[(int) (r % 3)], 0, FLAGS[(int) (r % 3)].length);
        byte[] status = STATUSES[(int) (r % 2)];
        ((BytesColumnVector) cols[9]).setRef(row, status, 0, status.length);
        ((LongColumnVector) cols[10]).vector[row] = EPOCH_1992 + r * 7907 % 2500;
        ((LongColumnVector) cols[11]).vector[row] = EPOCH_1992 + r * 7927 % 2500;
        ((LongColumnVector) cols[12]).vector[row] = EPOCH_1992 + r * 7933 % 2500;
        byte[] instruction = INSTRUCTIONS[(int) (r % 4)];
        ((BytesColumnVector) cols[13]).setRef(row, instruction, 0, instruction.length);
        byte[] mode = MODES[(int) (r % 7)];
        ((BytesColumnVector) cols[14]).setRef(row, mode, 0, mode.length);
        ((BytesColumnVector) cols[15]).setVal(row, ("carefully " + WORDS[(int) (r * 13 % 4)]
                + " deposits " + r * 48271 % 100000).getBytes(StandardCharsets.UTF_8));
    }

    private static byte[][] bytes(String... values) {
        byte[][] out = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            out[i] = values[i].getBytes(StandardCharsets.UTF_8);
        }
        return out;
    }

    private static void fill(ColumnVector[] cols, int row, int r) {
        ((LongColumnVector) cols[0]).vector[row] = r;
        if (r % 7 == 3) {
            for (int c = 1; c < cols.length; c++) {
                cols[c].noNulls = false;
                cols[c].isNull[row] = true;
            }
            return;
        }
        for (int c = 1; c < cols.length; c++) {
            cols[c].isNull[row] = false;
        }
        ((LongColumnVector) cols[1]).vector[row] = r * 1_000_000_000L;
        ((BytesColumnVector) cols[2]).setVal(row, ("s" + r + "ü").getBytes(StandardCharsets.UTF_8));
        ((DecimalColumnVector) cols[3]).set(row, HiveDecimal.create(BigDecimal.valueOf(r * 125L, 2)));
        ((LongColumnVector) cols[4]).vector[row] = LocalDate.of(2020, 1, 1).toEpochDay() + r;
        ((DoubleColumnVector) cols[5]).vector[row] = r / 8.0;
        ((LongColumnVector) cols[6]).vector[row] = r % 2 == 0 ? 1 : 0;
        ((BytesColumnVector) cols[7]).setVal(row, ("c" + r % 10).getBytes(StandardCharsets.UTF_8));
        ((TimestampColumnVector) cols[8]).set(row,
                Timestamp.valueOf(LocalDateTime.of(2020, 6, 1, 1, 30).plusMinutes(r)));
        ListColumnVector list = (ListColumnVector) cols[9];
        list.offsets[row] = list.childCount;
        list.lengths[row] = 2;
        list.childCount += 2;
        list.child.ensureSize(list.childCount, true);
        ((LongColumnVector) list.child).vector[(int) list.offsets[row]] = r;
        ((LongColumnVector) list.child).vector[(int) list.offsets[row] + 1] = r + 1;
        StructColumnVector struct = (StructColumnVector) cols[10];
        ((LongColumnVector) struct.fields[0]).vector[row] = -r;
        ((BytesColumnVector) struct.fields[1]).setVal(row, ("y" + r).getBytes(StandardCharsets.UTF_8));
        MapColumnVector map = (MapColumnVector) cols[11];
        map.offsets[row] = map.childCount;
        map.lengths[row] = 1;
        map.childCount += 1;
        map.keys.ensureSize(map.childCount, true);
        map.values.ensureSize(map.childCount, true);
        ((BytesColumnVector) map.keys).setVal((int) map.offsets[row], "k".getBytes(StandardCharsets.UTF_8));
        ((LongColumnVector) map.values).vector[(int) map.offsets[row]] = r;
    }
}
