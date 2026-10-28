package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.flight.FlightDescriptor;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Reads the query of a plain Flight descriptor (opt-in {@code plainFlightCommands}): a command
 * that is SQL text instead of a Flight SQL message, or a path naming a table, as generic Flight
 * clients (ClickHouse's {@code arrowFlight}) send them.
 */
public final class PlainFlightCommands {

    /** Type URL prefix of the Flight SQL messages, which keep their own handling. */
    static final String FLIGHT_SQL_TYPE_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql.";

    /** One part of a table name in a path: a plain identifier, so the SQL built is a table read. */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");

    /** Most parts of a table name (catalog, schema, table). */
    private static final int MAX_NAME_PARTS = 3;

    private PlainFlightCommands() {
    }

    /**
     * Tells whether a command is a Flight SQL message.
     *
     * @param command command bytes
     * @return true for a protobuf {@code Any} carrying a Flight SQL type
     */
    static boolean isFlightSql(byte[] command) {
        try {
            return Any.parseFrom(command).getTypeUrl().startsWith(FLIGHT_SQL_TYPE_PREFIX);
        } catch (InvalidProtocolBufferException e) {
            return false;
        }
    }

    /**
     * Returns the query of a plain descriptor.
     *
     * @param descriptor request descriptor
     * @return the SQL text of a plain command, {@code SELECT * FROM <name>} for a path, or null
     *         for a Flight SQL command (handled as before)
     * @throws IllegalArgumentException for a command that is not UTF-8 text or is blank, and for
     *         a path that is not a table name of plain identifiers
     */
    public static String query(FlightDescriptor descriptor) {
        if (descriptor.isCommand()) {
            byte[] command = descriptor.getCommand();
            if (isFlightSql(command)) {
                return null;
            }
            String sql = utf8(command).strip();
            if (sql.isEmpty()) {
                throw new IllegalArgumentException("Empty command: send SQL text or a Flight SQL command");
            }
            return sql;
        }
        return "SELECT * FROM " + tableName(descriptor.getPath());
    }

    /**
     * Joins the path of a descriptor into a dotted table name.
     *
     * @param path path elements; each may itself be dotted ({@code schema.table})
     * @return name such as {@code schema.table}
     * @throws IllegalArgumentException when a part is not a plain identifier or there are too many
     */
    private static String tableName(List<String> path) {
        List<String> parts = new ArrayList<>();
        for (String element : path) {
            for (String part : element.split("\\.", -1)) {
                if (!IDENTIFIER.matcher(part).matches()) {
                    throw new IllegalArgumentException("A descriptor path names a table as schema.table"
                            + " of plain identifiers (letters, digits, _); use a command with SQL for"
                            + " other names");
                }
                parts.add(part);
            }
        }
        if (parts.isEmpty() || parts.size() > MAX_NAME_PARTS) {
            throw new IllegalArgumentException("A descriptor path names one table as schema.table");
        }
        return String.join(".", parts);
    }

    /**
     * Decodes strict UTF-8.
     *
     * @param bytes command bytes
     * @return text
     * @throws IllegalArgumentException for malformed UTF-8
     */
    private static String utf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("The command is neither a Flight SQL command nor UTF-8 SQL text", e);
        }
    }
}
