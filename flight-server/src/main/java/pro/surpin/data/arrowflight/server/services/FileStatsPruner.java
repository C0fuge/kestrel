package pro.surpin.data.arrowflight.server.services;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.SQLDialect;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.QOM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

/**
 * Leaves out the Parquet files of a directory table whose footer statistics show that a query's
 * WHERE clause selects none of their rows, before DuckDB opens them (item 39 (c): a point query
 * opened every file of its table, tens of NameNode round trips on HDFS).
 *
 * <p>Only comparisons of a column with a literal are used ({@code =, <, <=, >, >=, BETWEEN,
 * IN}), combined with AND and OR, on columns stored as signed 32/64-bit integers (no logical
 * type or a signed integer type) or dates, against integral number literals (integer columns)
 * or date literals ({@code DATE '...'}, {@code CAST('...' AS DATE)}, a {@code yyyy-mm-dd}
 * string; date columns). Anything else - NOT, other operators or types, casts of columns,
 * parameters, NULL - counts as "may select rows". A file is only left out when some row group
 * of it lacks no statistics for the column and the literal lies outside the file's range of
 * non-null values; a file without statistics for the column, with only nulls in it, or with
 * columns (names or types) other than the table's first file is always kept, so that DuckDB
 * binds the kept files with the view's column types.
 *
 * <p>Footers are read through the store (the Java Parquet reader) once per file version (path,
 * length, modification time) and kept, least recently used dropped first.
 */
final class FileStatsPruner {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileStatsPruner.class);

    /** A column reference as jOOQ renders it with quoted names: "c" or "t"."c". */
    private static final Pattern COLUMN = Pattern.compile("^(?:\"[^\"]*\"\\.)*\"([^\"]*)\"$");
    /** A date literal written as text. */
    private static final Pattern DATE_TEXT = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

    /** How a column's footer bounds are compared. */
    enum Kind { INTEGER, DATE }

    /**
     * Range of a column's non-null values in one file.
     *
     * @param kind how the column is compared
     * @param min smallest value (days since the epoch for dates)
     * @param max largest value
     */
    record Bounds(Kind kind, long min, long max) {
    }

    /**
     * What a file's footer says.
     *
     * @param columns top-level columns in file order, with their repetition and types
     * @param bounds lower-case column name to its bounds; a column without usable bounds is absent
     */
    record FileStats(List<String> columns, Map<String, Bounds> bounds) {
    }

    private final int maxFiles;
    private final int maxCached;
    private final Map<String, FileStats> cache;
    private final DSLContext render = DSL.using(SQLDialect.DEFAULT,
            new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));
    private final Set<String> tooManyLogged = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Creates a pruner.
     *
     * @param maxFiles tables with more files than this are not pruned (their footers would cost
     *        the first query too much)
     * @param maxCached footers kept at most
     */
    FileStatsPruner(int maxFiles, int maxCached) {
        this.maxFiles = maxFiles;
        this.maxCached = maxCached;
        this.cache = new LinkedHashMap<>(256, 0.75f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, FileStats> eldest) {
                return size() > FileStatsPruner.this.maxCached;
            }
        };
    }

    /**
     * Returns the files a WHERE clause may select rows of.
     *
     * @param store store the files are read through
     * @param table table name, for logs
     * @param files the table's files, the first one the file its view was described from
     * @param filter WHERE clause as the query parser rendered it
     * @return the files kept, in their order, or null when none is left out
     * @throws IOException when a footer cannot be read
     */
    List<DataFile> keep(DataStore store, String table, List<DataFile> files, String filter)
            throws IOException {
        return keep(store, table, files, filter, null);
    }

    /**
     * Returns the files a WHERE clause may select rows of, reading its condition as the query
     * parser parsed it instead of parsing the rendered text again (item 40 (b)).
     *
     * @param store store the files are read through
     * @param table table name, for logs
     * @param files the table's files, the first one the file its view was described from
     * @param filter WHERE clause as the query parser rendered it
     * @param parsed the condition {@code filter} was rendered from, or null to parse {@code filter}
     * @return the files kept, in their order, or null when none is left out
     * @throws IOException when a footer cannot be read
     */
    List<DataFile> keep(DataStore store, String table, List<DataFile> files, String filter,
            Condition parsed) throws IOException {
        return keep(store, table, files, filter, parsed, Map.of());
    }

    /**
     * Returns the files a WHERE clause may select rows of, as {@link #keep(DataStore, String,
     * List, String, Condition)} does, with stand-in literals of the condition read as other
     * values: the condition parsed once for every text of one shape (item 40 (b)).
     *
     * @param store store the files are read through
     * @param table table name, for logs
     * @param files the table's files, the first one the file its view was described from
     * @param filter WHERE clause as the query parser rendered it
     * @param parsed the condition {@code filter} was rendered from, or null to parse {@code filter}
     * @param binds the value each stand-in integer literal of {@code parsed} stands for
     * @return the files kept, in their order, or null when none is left out
     * @throws IOException when a footer cannot be read
     */
    List<DataFile> keep(DataStore store, String table, List<DataFile> files, String filter,
            Condition parsed, Map<Long, Long> binds) throws IOException {
        if (parsed == null && !binds.isEmpty()) {
            throw new IllegalArgumentException("stand-in literals need the parsed condition");
        }
        if (filter == null || filter.isBlank() || files.size() < 2 || store == null) {
            return null;
        }
        if (files.size() > maxFiles) {
            if (tooManyLogged.add(table)) {
                LOGGER.info("node={} pruning=skipTable table={} files={} maxFiles={}",
                        LogUtil.node(), table, files.size(), maxFiles);
            }
            return null;
        }
        Condition condition = parsed;
        try {
            condition = condition != null ? condition : DSL.using(SQLDialect.DEFAULT,
                    new Settings().withParseNameCase(ParseNameCase.AS_IS)).parser()
                    .parseCondition(filter);
        } catch (RuntimeException e) {
            return null;
        }
        Set<String> used = new HashSet<>();
        columnsOf(condition, used);
        if (used.isEmpty()) {
            return null;   // no comparison of a column with a literal
        }
        long t = LogUtil.mark();
        List<FileStats> stats = new ArrayList<>(files.size());
        for (DataFile file : files) {
            stats.add(stats(store, file));
        }
        List<String> columns = stats.get(0).columns();
        List<DataFile> kept = new ArrayList<>(files.size());
        for (int i = 0; i < files.size(); i++) {
            FileStats s = stats.get(i);
            if (!s.columns().equals(columns) || mayMatch(condition, s.bounds(), binds)) {
                kept.add(files.get(i));
            }
        }
        LogUtil.logTiming(t, "pruning.files", "table=" + table + " kept=" + kept.size()
                + " files=" + files.size());
        return kept.size() == files.size() ? null : kept;
    }

    /**
     * Returns the statistics of one file, from the cache or its footer.
     *
     * @param store store
     * @param file file
     * @return statistics
     * @throws IOException when the footer cannot be read
     */
    private FileStats stats(DataStore store, DataFile file) throws IOException {
        String key = file.location() + "\u0000" + file.size() + "\u0000" + file.modificationTime();
        synchronized (cache) {
            FileStats cached = cache.get(key);
            if (cached != null) {
                return cached;
            }
        }
        FileStats read;
        try (ParquetFileReader reader = ParquetFileReader.open(store.inputFile(file))) {
            read = footerStats(reader.getFooter().getFileMetaData().getSchema().getFields(),
                    reader.getFooter().getBlocks());
        }
        synchronized (cache) {
            cache.put(key, read);
        }
        return read;
    }

    /**
     * Collects the bounds of a footer's integer and date columns.
     *
     * @param fields top-level fields of the file schema
     * @param blocks row groups
     * @return statistics
     */
    static FileStats footerStats(List<Type> fields, List<BlockMetaData> blocks) {
        List<String> columns = new ArrayList<>(fields.size());
        Map<String, Kind> kinds = new HashMap<>();
        Set<String> seen = new HashSet<>();
        Set<String> clashing = new HashSet<>();
        for (Type field : fields) {
            columns.add(field.toString());   // name, repetition, types: how DuckDB binds it
            String lower = field.getName().toLowerCase(Locale.ROOT);
            if (!seen.add(lower)) {
                clashing.add(lower);   // DuckDB ignores case: which one a name means is unclear
            }
            if (field.isPrimitive()) {
                Kind kind = kind(field.asPrimitiveType());
                if (kind != null) {
                    kinds.put(lower, kind);
                }
            }
        }
        clashing.forEach(kinds::remove);
        Map<String, long[]> ranges = new HashMap<>();
        Set<String> unknown = new HashSet<>();
        for (BlockMetaData block : blocks) {
            if (block.getRowCount() == 0) {
                continue;
            }
            Set<String> inBlock = new HashSet<>();
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                if (chunk.getPath().size() != 1) {
                    continue;
                }
                String lower = chunk.getPath().toDotString().toLowerCase(Locale.ROOT);
                if (!kinds.containsKey(lower)) {
                    continue;
                }
                inBlock.add(lower);
                Statistics<?> s = chunk.getStatistics();
                if (s == null || s.isEmpty() || !s.hasNonNullValue()
                        || !(s.genericGetMin() instanceof Number low)
                        || !(s.genericGetMax() instanceof Number high)) {
                    unknown.add(lower);   // no statistics, or only nulls: never prune on it
                    continue;
                }
                long[] range = ranges.computeIfAbsent(lower,
                        k -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
                range[0] = Math.min(range[0], low.longValue());
                range[1] = Math.max(range[1], high.longValue());
            }
            for (String column : kinds.keySet()) {
                if (!inBlock.contains(column)) {
                    unknown.add(column);
                }
            }
        }
        Map<String, Bounds> bounds = new HashMap<>();
        for (Map.Entry<String, long[]> range : ranges.entrySet()) {
            if (!unknown.contains(range.getKey())) {
                bounds.put(range.getKey(), new Bounds(kinds.get(range.getKey()),
                        range.getValue()[0], range.getValue()[1]));
            }
        }
        return new FileStats(List.copyOf(columns), Map.copyOf(bounds));
    }

    /**
     * Tells how a column's bounds are compared.
     *
     * @param type physical and logical type
     * @return kind, or null when the column's bounds are not used
     */
    static Kind kind(PrimitiveType type) {
        PrimitiveType.PrimitiveTypeName physical = type.getPrimitiveTypeName();
        if (physical != PrimitiveType.PrimitiveTypeName.INT32
                && physical != PrimitiveType.PrimitiveTypeName.INT64) {
            return null;
        }
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (logical == null) {
            return Kind.INTEGER;
        }
        if (logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation integer) {
            return integer.isSigned() ? Kind.INTEGER : null;   // unsigned: widened in the views
        }
        if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation
                && physical == PrimitiveType.PrimitiveTypeName.INT32) {
            return Kind.DATE;
        }
        return null;   // decimals, times, timestamps: not used
    }

    /**
     * Tells whether a condition may hold for some row of a file. False only when the condition
     * can be TRUE for no row: AND and OR are followed, every other expression may hold.
     *
     * @param condition condition
     * @param bounds the file's bounds by lower-case column name
     * @return false when the file can be left out
     */
    boolean mayMatch(Condition condition, Map<String, Bounds> bounds) {
        return mayMatch(condition, bounds, Map.of());
    }

    /**
     * Tells whether a condition may hold for some row of a file, as
     * {@link #mayMatch(Condition, Map)} does, reading stand-in literals as other values.
     *
     * @param condition condition
     * @param bounds the file's bounds by lower-case column name
     * @param binds the value each stand-in integer literal stands for
     * @return false when the file can be left out
     */
    boolean mayMatch(Condition condition, Map<String, Bounds> bounds, Map<Long, Long> binds) {
        if (condition instanceof QOM.And and) {
            return mayMatch(and.$arg1(), bounds, binds) && mayMatch(and.$arg2(), bounds, binds);
        }
        if (condition instanceof QOM.Or or) {
            return mayMatch(or.$arg1(), bounds, binds) || mayMatch(or.$arg2(), bounds, binds);
        }
        if (condition instanceof QOM.Eq<?> eq) {
            return compare(eq.$arg1(), eq.$arg2(), bounds, binds, Op.EQ);
        }
        if (condition instanceof QOM.Lt<?> lt) {
            return compare(lt.$arg1(), lt.$arg2(), bounds, binds, Op.LT);
        }
        if (condition instanceof QOM.Le<?> le) {
            return compare(le.$arg1(), le.$arg2(), bounds, binds, Op.LE);
        }
        if (condition instanceof QOM.Gt<?> gt) {
            return compare(gt.$arg1(), gt.$arg2(), bounds, binds, Op.GT);
        }
        if (condition instanceof QOM.Ge<?> ge) {
            return compare(ge.$arg1(), ge.$arg2(), bounds, binds, Op.GE);
        }
        if (condition instanceof QOM.Between<?> between && !between.$symmetric()) {
            return compare(between.$arg1(), between.$arg2(), bounds, binds, Op.GE)
                    && compare(between.$arg1(), between.$arg3(), bounds, binds, Op.LE);
        }
        if (condition instanceof QOM.InList<?> in) {
            for (Field<?> element : in.$arg2()) {
                if (compare(in.$arg1(), element, bounds, binds, Op.EQ)) {
                    return true;
                }
            }
            return in.$arg2().isEmpty();
        }
        return true;
    }

    /** Comparison operators, left operand first. */
    enum Op {
        EQ, LT, LE, GT, GE;

        /**
         * Returns the operator with its operands swapped ({@code 5 < c} is {@code c > 5}).
         *
         * @return swapped operator
         */
        Op swapped() {
            return switch (this) {
                case EQ -> EQ;
                case LT -> GT;
                case LE -> GE;
                case GT -> LT;
                case GE -> LE;
            };
        }
    }

    /**
     * Tells whether "column op literal" may hold for some value in a file's bounds.
     *
     * @param left left operand
     * @param right right operand
     * @param bounds file bounds
     * @param op operator
     * @return false only when no value of the file's range satisfies it
     */
    private boolean compare(Field<?> left, Field<?> right, Map<String, Bounds> bounds,
            Map<Long, Long> binds, Op op) {
        String column = column(left);
        Field<?> literal = right;
        Op o = op;
        if (column == null) {
            column = column(right);
            literal = left;
            o = op.swapped();
        }
        if (column == null) {
            return true;
        }
        Bounds b = bounds.get(column);
        if (b == null) {
            return true;
        }
        BigDecimal value = literal(literal, b.kind(), binds);
        if (value == null) {
            return true;
        }
        BigDecimal min = BigDecimal.valueOf(b.min());
        BigDecimal max = BigDecimal.valueOf(b.max());
        return switch (o) {
            case EQ -> value.compareTo(min) >= 0 && value.compareTo(max) <= 0;
            case LT -> min.compareTo(value) < 0;
            case LE -> min.compareTo(value) <= 0;
            case GT -> max.compareTo(value) > 0;
            case GE -> max.compareTo(value) >= 0;
        };
    }

    /**
     * Reads a plain column reference.
     *
     * @param field expression
     * @return lower-case column name, or null when it is no plain column
     */
    private String column(Field<?> field) {
        if (field instanceof Param<?> || field instanceof QOM.Cast<?> || field instanceof QOM.Neg<?>) {
            return null;
        }
        Matcher m = COLUMN.matcher(render.render(field).trim());
        return m.matches() ? m.group(1).toLowerCase(Locale.ROOT) : null;
    }

    /**
     * Reads a literal as a number comparable with a column's bounds.
     *
     * @param field expression
     * @param kind the column's kind
     * @return value (days since the epoch for dates), or null when it is no usable literal
     */
    static BigDecimal literal(Field<?> field, Kind kind) {
        return literal(field, kind, Map.of());
    }

    /**
     * Reads a literal as {@link #literal(Field, Kind)} does, a stand-in integer literal as the
     * value it stands for.
     *
     * @param field expression
     * @param kind the column's kind
     * @param binds the value each stand-in integer literal stands for (also negated)
     * @return value, or null when it is no usable literal
     */
    static BigDecimal literal(Field<?> field, Kind kind, Map<Long, Long> binds) {
        if (field instanceof QOM.Neg<?> neg) {
            BigDecimal inner = kind == Kind.INTEGER ? literal(neg.$arg1(), kind, binds) : null;
            return inner == null ? null : inner.negate();
        }
        if (field instanceof QOM.Cast<?> cast) {
            if (kind == Kind.DATE && cast.$field() instanceof Param<?> param
                    && param.getValue() instanceof String text
                    && cast.$dataType().getSQLDataType() == org.jooq.impl.SQLDataType.DATE) {
                return dateText(text);
            }
            return null;   // other casts change what is compared
        }
        if (!(field instanceof Param<?> param) || param.getValue() == null) {
            return null;   // not a literal, or NULL / a parameter
        }
        Object value = param.getValue();
        if (value instanceof Long stand && binds.containsKey(stand)) {
            value = binds.get(stand);   // a client's integer literal, parsed as its stand-in
        }
        if (kind == Kind.INTEGER) {
            BigDecimal number;
            if (value instanceof Integer || value instanceof Long || value instanceof Short
                    || value instanceof Byte) {
                number = BigDecimal.valueOf(((Number) value).longValue());
            } else if (value instanceof BigInteger big) {
                number = new BigDecimal(big);
            } else if (value instanceof BigDecimal decimal) {
                number = decimal;
            } else {
                return null;   // text, floating point, ...: DuckDB's conversion decides
            }
            return number.stripTrailingZeros().scale() > 0 ? null : number;
        }
        if (value instanceof java.sql.Date date) {
            return BigDecimal.valueOf(date.toLocalDate().toEpochDay());
        }
        if (value instanceof LocalDate date) {
            return BigDecimal.valueOf(date.toEpochDay());
        }
        if (value instanceof String text) {
            return dateText(text);
        }
        return null;
    }

    private static BigDecimal dateText(String text) {
        if (!DATE_TEXT.matcher(text).matches()) {
            return null;
        }
        try {
            return BigDecimal.valueOf(LocalDate.parse(text).toEpochDay());
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Collects the columns a condition compares with literals (whether pruning may help).
     *
     * @param condition condition
     * @param out lower-case column names
     */
    private void columnsOf(Condition condition, Set<String> out) {
        if (condition instanceof QOM.And and) {
            columnsOf(and.$arg1(), out);
            columnsOf(and.$arg2(), out);
        } else if (condition instanceof QOM.Or or) {
            columnsOf(or.$arg1(), out);
            columnsOf(or.$arg2(), out);
        } else if (condition instanceof QOM.Eq<?> eq) {
            addColumn(eq.$arg1(), eq.$arg2(), out);
        } else if (condition instanceof QOM.Lt<?> lt) {
            addColumn(lt.$arg1(), lt.$arg2(), out);
        } else if (condition instanceof QOM.Le<?> le) {
            addColumn(le.$arg1(), le.$arg2(), out);
        } else if (condition instanceof QOM.Gt<?> gt) {
            addColumn(gt.$arg1(), gt.$arg2(), out);
        } else if (condition instanceof QOM.Ge<?> ge) {
            addColumn(ge.$arg1(), ge.$arg2(), out);
        } else if (condition instanceof QOM.Between<?> between) {
            addColumn(between.$arg1(), between.$arg2(), out);
        } else if (condition instanceof QOM.InList<?> in && !in.$arg2().isEmpty()) {
            addColumn(in.$arg1(), in.$arg2().get(0), out);
        }
    }

    private void addColumn(Field<?> a, Field<?> b, Set<String> out) {
        String column = column(a) != null ? column(a) : column(b);
        if (column != null) {
            out.add(column);
        }
    }
}
