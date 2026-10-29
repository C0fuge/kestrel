package pro.surpin.data.arrowflight.server.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Several load reservations of identical endpoint tickets, kept under one reservation key.
 * Endpoint tickets are deterministic: clients that plan the same query at the same time get the
 * same ticket, so one key holds a reservation per planned endpoint. A key with one reservation
 * holds the {@link HandleState} itself, as before; this value holds two or more.
 */
public final class LoadReservations implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** The reservations, each with its node and bytes. */
    private final ArrayList<HandleState> units;

    /**
     * Creates the value.
     *
     * @param units reservations, at least two
     */
    private LoadReservations(List<HandleState> units) {
        this.units = new ArrayList<>(units);
    }

    /**
     * Lists the reservations a reservation-map value holds.
     *
     * @param value value of a reservation key, may be null
     * @return reservations, empty for null or an unknown value
     */
    public static List<HandleState> units(Serializable value) {
        if (value instanceof HandleState state) {
            return List.of(state);
        }
        if (value instanceof LoadReservations reservations) {
            return List.copyOf(reservations.units);
        }
        return List.of();
    }

    /**
     * Builds the value that holds reservations.
     *
     * @param units reservations
     * @return null when empty, the reservation itself when one, else a {@code LoadReservations}
     */
    public static Serializable of(List<HandleState> units) {
        if (units.isEmpty()) {
            return null;
        }
        return units.size() == 1 ? units.get(0) : new LoadReservations(units);
    }
}
