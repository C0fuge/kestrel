package pro.surpin.data.arrowflight.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Compares text a test produces with a golden file under {@code src/test/resources/golden}.
 * Golden files pin behaviour that refactorings must keep byte for byte. Run the tests with
 * {@code -Dgolden.update=true} to write them anew, then review the diff before committing.
 */
public final class Golden {

    /** Directory the golden files are written to, relative to the module directory. */
    private static final Path SOURCE_DIR = Path.of("src", "test", "resources", "golden");

    /** Not instantiable. */
    private Golden() {
    }

    /**
     * Asserts that text equals the golden file of that name, or writes the file when
     * {@code golden.update} is set.
     *
     * @param name golden file name, for example {@code duckdb-setup.txt}
     * @param actual text produced by the test
     */
    public static void assertMatches(String name, String actual) {
        if (Boolean.getBoolean("golden.update")) {
            try {
                Files.createDirectories(SOURCE_DIR);
                Files.writeString(SOURCE_DIR.resolve(name), actual, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }
        String expected;
        try (InputStream in = Golden.class.getResourceAsStream("/golden/" + name)) {
            if (in == null) {
                fail("No golden file golden/" + name + "; run with -Dgolden.update=true to create it");
                return;
            }
            expected = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertEquals(expected, actual, "differs from golden/" + name);
    }
}
